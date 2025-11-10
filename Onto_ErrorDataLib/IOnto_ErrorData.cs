// Onto_ErrorDataLib/IOnto_ErrorData.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Npgsql;
using ConnectInfo;

namespace Onto_ErrorDataLib
{
    /*──────────────────────── Logger ───────────────────────*/
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebug(bool enabled) { _debugEnabled = enabled; }

        private static readonly object _sync = new object();
        private static readonly string _dir = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static string PathOf(string sfx) => System.IO.Path.Combine(_dir, $"{DateTime.Now:yyyyMMdd}_{sfx}.log");

        private static void Write(string s, string m)
        {
            try
            {
                lock (_sync)
                {
                    System.IO.Directory.CreateDirectory(_dir);
                    var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ErrorData] {m}{Environment.NewLine}";
                    System.IO.File.AppendAllText(PathOf(s), line, System.Text.Encoding.UTF8);
                }
            }
            catch { /* 로깅 실패 무시 */ }
        }

        public static void Event(string m) { Write("event", m); }
        public static void Error(string m) { Write("error", m); }
        public static void Debug(string m)
        {
            if (_debugEnabled) Write("debug", m);
        }
    }

    /*──────────────────────── Interface ─────────────────────*/
    // [추가] 기존 플러그인들과 동일한 시그니처(호환성 보장)
    public interface IOnto_ErrorData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null);
        void StartWatch(string folderPath);
        void StopWatch();
    }

    /*──────────────────────── Implementation ───────────────*/
    public class Onto_ErrorData : IOnto_ErrorData
    {
        /* 상태값/상수 */
        private FileSystemWatcher _fw;
        private DateTime _lastEvt = DateTime.MinValue;
        private static readonly Dictionary<string, long> _lastLen =
            new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        private readonly string _pluginName = "Onto_ErrorData";
        public string PluginName { get { return _pluginName; } }

        static Onto_ErrorData()
        {
#if NETCOREAPP || NET5_0_OR_GREATER
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
#endif
        }

        #region === Public API ===
        // [추가] ucUploadPanel 호출 규약과 동일한 메서드 시그니처
        public void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null)
        {
            SimpleLogger.Event("Process ▶ " + filePath);

            // 파일 준비 대기 (잠금 해제까지 재시도)
            if (!WaitForFileReady(filePath, 20, 500))
            {
                SimpleLogger.Event("SKIP – file still not ready ▶ " + filePath);
                return;
            }

            // Eqpid 로드
            string eqpid = GetEqpidFromSettings("Settings.ini");

            try
            {
                ProcessFile(filePath, eqpid);
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("Unhandled EX ▶ " + ex);
            }
        }

        public void StartWatch(string folderPath)
        {
            if (string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
            {
                SimpleLogger.Error("watch path invalid: " + folderPath);
                return;
            }

            StopWatch(); // 기존 인스턴스 정리

            _fw = new FileSystemWatcher(folderPath)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
                Filter = "*.log",
                IncludeSubdirectories = false,
                EnableRaisingEvents = true
            };

            _fw.Created += OnChanged;
            _fw.Changed += OnChanged;

            SimpleLogger.Event("Watcher started ▶ " + folderPath);
        }

        public void StopWatch()
        {
            if (_fw == null) return;

            _fw.EnableRaisingEvents = false;
            _fw.Created -= OnChanged;
            _fw.Changed -= OnChanged;
            _fw.Dispose();
            _fw = null;

            SimpleLogger.Event("Watcher stopped");
        }
        #endregion

        #region === Watcher Internal ===
        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            // 간단한 debounce (200ms)
            var now = DateTime.Now;
            if ((now - _lastEvt).TotalMilliseconds < 200) return;
            _lastEvt = now;

            // 파일 길이 변화 없으면 무시 (중복 이벤트 억제)
            try
            {
                if (File.Exists(e.FullPath))
                {
                    long len = new FileInfo(e.FullPath).Length;
                    long prev;
                    if (_lastLen.TryGetValue(e.FullPath, out prev) && prev == len)
                        return;
                    _lastLen[e.FullPath] = len;

                    // 비동기 처리
                    ThreadPool.QueueUserWorkItem(_ => ProcessAndUpload(e.FullPath));
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("Watcher EX ▶ " + ex.Message);
            }
        }
        #endregion

        #region === Core ===
        private void ProcessFile(string filePath, string eqpid)
        {
            // 파일 읽기 및 파싱 (기존 로직 유지)
            string raw = File.ReadAllText(filePath, Encoding.GetEncoding(949));
            var lines = raw.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
            var meta = ParseMeta(lines);
            if (!meta.ContainsKey("EqpId")) meta["EqpId"] = eqpid;

            // itm_info 업서트 (기존 로직)
            var infoTable = BuildInfoDataTable(meta);
            UploadItmInfoUpsert(infoTable);

            // Error 데이터 테이블 구성 (기존)
            var errorTable = BuildErrorDataTable(lines, eqpid);

            // [추가] 허용 Error ID 집합 로드
            HashSet<string> allowSet = LoadErrorFilterSet();

            // [추가] 필터링 적용 (허용 ID만 보존)
            int matched, skipped;
            DataTable filtered = ApplyErrorFilter(errorTable, allowSet, out matched, out skipped);

            // [추가] 필터링 결과 로그
            SimpleLogger.Event(string.Format("ErrorFilter ▶ total={0}, matched={1}, skipped={2}",
                                  errorTable != null ? errorTable.Rows.Count : 0, matched, skipped));

            // [수정] 허용된 행만 DB 적재
            if (filtered != null && filtered.Rows.Count > 0)
            {
                UploadDataTable(filtered, "plg_error");
            }
            else
            {
                SimpleLogger.Event("No rows after filter ▶ plg_error");
            }

            SimpleLogger.Event("Done ▶ " + Path.GetFileName(filePath));
        }

        // [추가] Error ID 정규화 (대소문자/공백 차이 제거)
        private static string NormalizeErrorId(object v)
        {
            if (v == null || v == DBNull.Value) return string.Empty;
            string s = v.ToString().Trim();
            return s.ToUpperInvariant();
        }

        // [추가] public.err_severity_map 에서 허용 Error ID 집합 로드
        private HashSet<string> LoadErrorFilterSet()
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            const string SQL = @"SELECT error_id FROM public.err_severity_map;";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand(SQL, conn))
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var id = rd.IsDBNull(0) ? string.Empty : rd.GetString(0);
                        id = NormalizeErrorId(id);
                        if (!string.IsNullOrEmpty(id)) set.Add(id);
                    }
                }
            }
            return set;
        }

        // [추가] DataTable 필터링: 허용 Error ID만 남김
        private DataTable ApplyErrorFilter(DataTable src, HashSet<string> allowSet, out int matched, out int skipped)
        {
            matched = 0; skipped = 0;
            if (src == null || src.Rows.Count == 0 || allowSet == null || allowSet.Count == 0)
            {
                skipped = (src != null) ? src.Rows.Count : 0;
                return src != null ? src.Clone() : new DataTable();
            }

            var dst = src.Clone();
            foreach (DataRow r in src.Rows)
            {
                string id = NormalizeErrorId(r["error_id"]);
                if (allowSet.Contains(id))
                {
                    dst.ImportRow(r);
                    matched++;
                }
                else
                {
                    skipped++;
                }
            }
            return dst;
        }

        private void UploadItmInfoUpsert(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0) return;

            var r = dt.Rows[0];
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            // [추가] 변경 여부 선판단: 동일 eqpid에 동일 속성 조합이 이미 존재하면 스킵
            if (!IsInfoChanged(dt))
            {
                SimpleLogger.Event("itm_info unchanged ▶ eqpid=" + (r["eqpid"] ?? ""));
                return;
            }

            // [유지/수정] serv_ts 보정(밀리초 제거)
            DateTime srcDate = DateTime.Now;
            var dv = r["date"];
            if (dv != null && dv != DBNull.Value)
            {
                DateTime dtParsed;
                if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out dtParsed))
                    srcDate = dtParsed;
            }
            var srv = ITM_Agent.Services.TimeSyncProvider
                          .Instance.ToSynchronizedKst(srcDate);
            srv = new DateTime(srv.Year, srv.Month, srv.Day, srv.Hour, srv.Minute, srv.Second);

            // [수정] 단순 INSERT — 충돌 대상(UNIQUE/PK on eqpid)이 없어야 함
            const string SQL = @"
                INSERT INTO public.itm_info
                    (eqpid, system_name, system_model, serial_num, application, version, db_version, ""date"", serv_ts)
                VALUES
                    (@eqpid, @system_name, @system_model, @serial_num, @application, @version, @db_version, @date, @serv_ts);
            ";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand(SQL, conn))
                {
                    cmd.Parameters.AddWithValue("@eqpid", r["eqpid"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@system_name", r["system_name"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@system_model", r["system_model"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@serial_num", r["serial_num"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@application", r["application"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@version", r["version"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@db_version", r["db_version"] ?? (object)DBNull.Value);

                    object dateParam = DBNull.Value;
                    if (dv != null && dv != DBNull.Value)
                    {
                        DateTime dtParsed;
                        if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                            CultureInfo.InvariantCulture, DateTimeStyles.None, out dtParsed))
                            dateParam = dtParsed;
                        else
                            dateParam = dv.ToString();
                    }
                    cmd.Parameters.AddWithValue("@date", dateParam);
                    cmd.Parameters.AddWithValue("@serv_ts", srv);

                    cmd.ExecuteNonQuery();
                }
            }
            SimpleLogger.Event("itm_info inserted ▶ eqpid=" + (r["eqpid"] ?? ""));
        }

        // 파일 상단의 "키:," 형태 메타데이터 파싱 + DATE 형식 변환
        private Dictionary<string, string> ParseMeta(string[] lines)
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < lines.Length; i++)
            {
                var ln = lines[i];
                int idx = ln.IndexOf(":,");
                if (idx <= 0) continue;

                string key = ln.Substring(0, idx).Trim();
                string val = ln.Substring(idx + 2).Trim();
                if (key.Length == 0) continue;

                // 'EXPORT_TYPE' 은 스킵 (요구사항)
                if (string.Equals(key, "EXPORT_TYPE", StringComparison.OrdinalIgnoreCase))
                    continue;

                d[key] = val;
            }

            // DATE: M/d/yyyy H:m:s → yyyy-MM-dd HH:mm:ss
            string ds;
            if (d.TryGetValue("DATE", out ds))
            {
                DateTime dt;
                if (DateTime.TryParseExact(ds, "M/d/yyyy H:m:s", CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                    d["DATE"] = dt.ToString("yyyy-MM-dd HH:mm:ss");
            }

            return d;
        }

        // itm_info DataTable 구성 (컬럼은 소문자 스네이크케이스로 통일)
        private DataTable BuildInfoDataTable(Dictionary<string, string> meta)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["DATE"] = "date",
                ["SYSTEM_NAME"] = "system_name",
                ["SYSTEM_MODEL"] = "system_model",
                ["SERIAL_NUM"] = "serial_num",
                ["APPLICATION"] = "application",
                ["VERSION"] = "version",
                ["DB_VERSION"] = "db_version",
                ["EqpId"] = "eqpid"
            };

            var dt = new DataTable();
            foreach (var c in map.Values) dt.Columns.Add(c, typeof(string));
            var dr = dt.NewRow();
            foreach (var kv in map)
                dr[kv.Value] = meta.TryGetValue(kv.Key, out string v) ? (object)v : DBNull.Value;
            dt.Rows.Add(dr);
            return dt;
        }

        // error DataTable 구성
        private DataTable BuildErrorDataTable(string[] lines, string eqpid)
        {
            var dt = new DataTable();
            dt.Columns.AddRange(new[]
            {
                new DataColumn("eqpid", typeof(string)),
                new DataColumn("error_id", typeof(string)),
                new DataColumn("time_stamp", typeof(DateTime)),
                new DataColumn("error_label", typeof(string)),
                new DataColumn("error_desc", typeof(string)),
                new DataColumn("millisecond", typeof(int)),
                new DataColumn("extra_message_1", typeof(string)),
                new DataColumn("extra_message_2", typeof(string)),
                new DataColumn("serv_ts", typeof(DateTime))                 // [추가] 보정된 서버시각(초단위)
            });

            var rg = new Regex(
                @"^(?<id>\w+),\s*(?<ts>[^,]+),\s*(?<lbl>[^,]+),\s*(?<desc>[^,]+),\s*(?<ms>\d+)(?:,\s*(?<extra>.*))?",
                RegexOptions.Compiled);

            foreach (var ln in lines)
            {
                var m = rg.Match(ln);
                if (!m.Success) continue;

                DateTime parsedTs;
                bool okTs = DateTime.TryParseExact(
                    m.Groups["ts"].Value.Trim(),
                    "dd-MMM-yy h:mm:ss tt",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out parsedTs);

                var dr = dt.NewRow();
                dr["eqpid"] = eqpid;
                dr["error_id"] = m.Groups["id"].Value.Trim();
                if (okTs) dr["time_stamp"] = parsedTs;
                dr["error_label"] = m.Groups["lbl"].Value.Trim();
                dr["error_desc"] = m.Groups["desc"].Value.Trim();

                int ms;
                if (int.TryParse(m.Groups["ms"].Value, out ms)) dr["millisecond"] = ms;

                dr["extra_message_1"] = m.Groups["extra"].Value.Trim();
                dr["extra_message_2"] = "";

                // [추가] serv_ts = 보정 + 밀리초 절삭
                var basis = okTs ? parsedTs : DateTime.Now;
                var srv = ITM_Agent.Services.TimeSyncProvider
                                .Instance.ToSynchronizedKst(basis);
                srv = new DateTime(srv.Year, srv.Month, srv.Day,
                                   srv.Hour, srv.Minute, srv.Second);
                dr["serv_ts"] = srv;

                dt.Rows.Add(dr);
            }
            return dt;
        }
        #endregion

        #region === DB Helper ===
        // itm_info 변경 여부 판단 (DATE 제외)
        private bool IsInfoChanged(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0) return false;
            var r = dt.Rows[0];

            string cs = DatabaseInfo.CreateDefault().GetConnectionString();
            const string SQL = @"
                SELECT 1
                FROM public.itm_info
                WHERE eqpid = @eqp
                  AND system_name IS NOT DISTINCT FROM @sn
                  AND system_model IS NOT DISTINCT FROM @sm
                  AND serial_num IS NOT DISTINCT FROM @snm
                  AND application IS NOT DISTINCT FROM @app
                  AND version IS NOT DISTINCT FROM @ver
                  AND db_version IS NOT DISTINCT FROM @dbv
                LIMIT 1;";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand(SQL, conn))
                {
                    cmd.Parameters.AddWithValue("@eqp", r["eqpid"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@sn", r["system_name"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@sm", r["system_model"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@snm", r["serial_num"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@app", r["application"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ver", r["version"] ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@dbv", r["db_version"] ?? (object)DBNull.Value);

                    object o = cmd.ExecuteScalar();
                    return o == null; // 존재하지 않으면 "변경됨"으로 간주 → INSERT 수행
                }
            }
        }

        // 공용 업로드 (WaferFlat/Prealign 스타일과 동일한 INSERT 루프)
        private int UploadDataTable(DataTable dt, string tableName)
        {
            if (dt == null || dt.Rows.Count == 0) return 0;

            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                using (var tx = conn.BeginTransaction())
                {
                    var cols = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    string colList = string.Join(",", cols.Select(c => "\"" + c + "\""));
                    string paramList = string.Join(",", cols.Select(c => "@" + c));

                    // [수정] 중복 시 예외 없이 건너뛰기
                    string sql = string.Format(
                        "INSERT INTO public.{0} ({1}) VALUES ({2}) ON CONFLICT DO NOTHING;",
                        tableName, colList, paramList);

                    using (var cmd = new NpgsqlCommand(sql, conn, tx))
                    {
                        foreach (var c in cols)
                            cmd.Parameters.Add(new NpgsqlParameter("@" + c, DbType.Object));

                        int inserted = 0;
                        try
                        {
                            foreach (DataRow r in dt.Rows)
                            {
                                foreach (var c in cols)
                                    cmd.Parameters["@" + c].Value = r[c] ?? DBNull.Value;

                                int affected = cmd.ExecuteNonQuery();
                                if (affected == 1) inserted++;
                            }
                            tx.Commit();

                            int skipped = dt.Rows.Count - inserted;
                            SimpleLogger.Debug(
                                string.Format("DB OK ▶ {0}, inserted={1}, total={2}", tableName, inserted, dt.Rows.Count));
                            if (skipped > 0)
                                SimpleLogger.Event("Duplicate entry skipped ▶ " + tableName + " (skipped=" + skipped + ")");

                            return inserted;
                        }
                        catch (PostgresException pex)
                        {
                            tx.Rollback();
                            var sb = new StringBuilder()
                                .AppendLine("PG CODE = " + pex.SqlState)
                                .AppendLine("Message  = " + pex.Message)
                                .AppendLine("SQL      = " + sql);
                            foreach (NpgsqlParameter p in cmd.Parameters)
                                sb.AppendLine(p.ParameterName + " = " + (p.Value ?? "NULL"));
                            SimpleLogger.Error("DB FAIL ▶ " + sb.ToString());
                            return 0;
                        }
                        catch (Exception ex)
                        {
                            tx.Rollback();
                            SimpleLogger.Error("DB FAIL ▶ " + ex);
                            return 0;
                        }
                    }
                }
            }
        }
        #endregion

        #region === Utility ===
        private bool WaitForFileReady(string path, int maxRetries, int delayMs)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                if (File.Exists(path))
                {
                    try
                    {
                        using (var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            return true;             // 잠금 해제 상태
                        }
                    }
                    catch (IOException)
                    {
                        // 여전히 잠겨 있음 → 재시도
                    }
                }
                Thread.Sleep(delayMs);
            }
            return false;
        }

        private string GetEqpidFromSettings(string iniPath)
        {
            try
            {
                string path = Path.IsPathRooted(iniPath)
                    ? iniPath
                    : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, iniPath);

                if (!File.Exists(path)) return string.Empty;

                foreach (var line in File.ReadLines(path))
                {
                    var trimmed = line.Trim();
                    if (trimmed.StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = trimmed.IndexOf('=');
                        if (idx > 0) return trimmed.Substring(idx + 1).Trim();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("GetEqpidFromSettings EX ▶ " + ex.Message);
            }
            return string.Empty;
        }
        #endregion
    }
}
