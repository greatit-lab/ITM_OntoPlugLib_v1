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
using ITM_Agent.Services;

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

    public interface IOnto_ErrorData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null);
    }

    public class Onto_ErrorData : IOnto_ErrorData
    {
        // [핵심] 증분 처리를 위한 마지막 파일 크기(Offset) 저장소
        // static으로 선언되어 Agent가 재시작되지 않는 한 메모리에 유지됩니다.
        private static readonly Dictionary<string, long> _lastLen =
            new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        private readonly string _pluginName = "Onto_ErrorData";
        public string PluginName { get { return _pluginName; } }

        // [추가] Req 3. 메타데이터 속성 (Agent가 리플렉션으로 이 이름을 찾아 읽음)
        public string DefaultTaskName => "Error";
        public string DefaultFileFilter => "*Error.dat";

        static Onto_ErrorData()
        {
            #if NETCOREAPP || NET5_0_OR_GREATER
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            #endif
        }

        #region === Public API ===

        // ucUploadPanel (탭 2)에서 호출하는 증분 처리 로직
        public void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null)
        {
            SimpleLogger.Event("Process (Incremental) ▶ " + filePath);
            string eqpid = GetEqpidFromSettings(arg1 as string ?? "Settings.ini");

            long prevLen = 0;
            long currLen = 0;
            string[] addedLines = null;
            string[] allLinesForMeta = null;

            try
            {
                // --- 증분 처리를 위한 길이 확인 및 파일 읽기 ---
                lock (_lastLen)
                {
                    _lastLen.TryGetValue(filePath, out prevLen);
                }

                // FileShare.ReadWrite로 증분 읽기 (공유 위반 해결)
                using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    currLen = fs.Length;

                    if (currLen == prevLen && prevLen > 0)
                    {
                        SimpleLogger.Debug("File length unchanged, skipping incremental process: " + filePath);
                        return;
                    }

                    if (currLen < prevLen)
                    {
                        SimpleLogger.Event("File truncated (Size decreased). Resetting offset: " + filePath);
                        prevLen = 0; // 파일이 새로 생성되거나 잘렸으므로 처음부터
                    }

                    // [메타데이터 처리] (prevLen == 0, 즉 최초 실행 시에만)
                    if (prevLen == 0)
                    {
                        using (var srMeta = new StreamReader(fs, Encoding.GetEncoding(949)))
                        {
                            string allTextForMeta = srMeta.ReadToEnd();
                            allLinesForMeta = allTextForMeta.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                            addedLines = allLinesForMeta; 
                        }
                    }
                    // [증분 데이터 처리] (메모리 안정화)
                    else
                    {
                        fs.Seek(prevLen, SeekOrigin.Begin);
                        using (var srAdded = new StreamReader(fs, Encoding.GetEncoding(949)))
                        {
                            string addedText = srAdded.ReadToEnd();
                            addedLines = addedText.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                        }
                    }
                } // FileStream 닫기 (잠금 해제)
                
                // --- ProcessFile 로직을 여기로 통합 ---

                // [메타데이터 처리] (prevLen == 0일 때만)
                if (prevLen == 0 && allLinesForMeta != null)
                {
                    var meta = ParseMeta(allLinesForMeta);
                    if (!meta.ContainsKey("EqpId")) meta["EqpId"] = eqpid;

                    var infoTable = BuildInfoDataTable(meta);
                    UploadItmInfoUpsert(infoTable);
                }

                // [오류 데이터 처리]
                if (addedLines == null || addedLines.Length == 0)
                {
                    SimpleLogger.Debug("No new lines detected.");
                }
                else
                {
                    var errorTable = BuildErrorDataTable(addedLines, eqpid);

                    HashSet<string> allowSet = LoadErrorFilterSet();
                    int matched, skipped;
                    DataTable filtered = ApplyErrorFilter(errorTable, allowSet, out matched, out skipped);

                    SimpleLogger.Event(string.Format("ErrorFilter (Incremental) ▶ read_lines={0}, matched={1}, skipped={2}",
                                          (addedLines ?? new string[0]).Length, matched, skipped));

                    if (filtered != null && filtered.Rows.Count > 0)
                    {
                        UploadDataTable(filtered, "plg_error");
                    }
                    else
                    {
                        SimpleLogger.Event("No rows after filter ▶ plg_error");
                    }
                }

                SimpleLogger.Event("Done (Incremental) ▶ " + Path.GetFileName(filePath));

                // --- 현재 파일 크기를 _lastLen에 갱신 ---
                lock (_lastLen)
                {
                    _lastLen[filePath] = currLen;
                }
            }
            catch (FileNotFoundException)
            {
                SimpleLogger.Debug("File not found (maybe deleted): " + filePath);
                lock (_lastLen) { _lastLen.Remove(filePath); }
            }
            catch (IOException ioEx)
            {
                SimpleLogger.Error($"IO Exception during processing {filePath} (retrying next time): {ioEx.Message}");
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX in ProcessAndUpload for {filePath} ▶ {ex.GetBaseException().Message}");
            }
        }

        #endregion

        #region === Core  ===
        // NormalizeErrorId
        private static string NormalizeErrorId(object v)
        {
            if (v == null || v == DBNull.Value) return string.Empty;
            string s = v.ToString().Trim();
            return s.ToUpperInvariant();
        }

        private HashSet<string> LoadErrorFilterSet()
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();

            const string SQL = @"SELECT error_id FROM public.err_severity_map;";

            try
            {
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
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("Failed to load ErrorFilterSet from DB: " + ex.Message);
            }
            return set;
        }

        private DataTable ApplyErrorFilter(DataTable src, HashSet<string> allowSet, out int matched, out int skipped)
        {
            matched = 0; skipped = 0;
            if (src == null || src.Rows.Count == 0)
            {
                return src != null ? src.Clone() : new DataTable();
            }

            if (allowSet == null || allowSet.Count == 0)
            {
                skipped = src.Rows.Count;
                return src.Clone();
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

            try
            {
                if (!IsInfoChanged(dt))
                {
                    SimpleLogger.Event("itm_info unchanged ▶ eqpid=" + (r["eqpid"] ?? ""));
                    return;
                }

                DateTime srcDate = DateTime.Now;
                var dv = r["date"];
                if (dv != null && dv != DBNull.Value)
                {
                    if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                        CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dtParsed))
                        srcDate = dtParsed;
                }
                var srv = ITM_Agent.Services.TimeSyncProvider
                              .Instance.ToSynchronizedKst(srcDate);
                srv = new DateTime(srv.Year, srv.Month, srv.Day, srv.Hour, srv.Minute, srv.Second);

                // [핵심 수정] 42P10 오류 해결: ON CONFLICT 구문 제거
                // IsInfoChanged()가 중복을 이미 확인했으므로, 단순 INSERT만 수행합니다.
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
                            if (DateTime.TryParseExact(dv.ToString(), "yyyy-MM-dd HH:mm:ss",
                                CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dtParsed))
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
            catch (Exception ex)
            {
                SimpleLogger.Error("UploadItmInfoUpsert failed: " + ex.Message);
            }
        }

        // ParseMeta
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

                if (string.Equals(key, "EXPORT_TYPE", StringComparison.OrdinalIgnoreCase))
                    continue;

                d[key] = val;
            }

            string ds;
            if (d.TryGetValue("DATE", out ds))
            {
                if (DateTime.TryParseExact(ds, "M/d/yyyy H:m:s", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dt))
                    d["DATE"] = dt.ToString("yyyy-MM-dd HH:mm:ss");
            }

            return d;
        }

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
                new DataColumn("serv_ts", typeof(DateTime))
            });

            var rg = new Regex(
                @"^(?<id>\w+),\s*(?<ts>[^,]+),\s*(?<lbl>[^,]+),\s*(?<desc>[^,]+),\s*(?<ms>\d+)(?:,\s*(?<extra>.*))?",
                RegexOptions.Compiled);

            foreach (var ln in lines)
            {
                var m = rg.Match(ln);
                if (!m.Success) continue;

                if (!DateTime.TryParseExact(
                    m.Groups["ts"].Value.Trim(),
                    "dd-MMM-yy h:mm:ss tt",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out DateTime parsedTs))
                {
                    continue;
                }

                var dr = dt.NewRow();
                dr["eqpid"] = eqpid;
                dr["error_id"] = m.Groups["id"].Value.Trim();
                dr["time_stamp"] = parsedTs;
                dr["error_label"] = m.Groups["lbl"].Value.Trim();
                dr["error_desc"] = m.Groups["desc"].Value.Trim();

                if (int.TryParse(m.Groups["ms"].Value, out int ms)) dr["millisecond"] = ms;

                dr["extra_message_1"] = m.Groups["extra"].Value.Trim();
                dr["extra_message_2"] = "";

                var srv = ITM_Agent.Services.TimeSyncProvider
                                .Instance.ToSynchronizedKst(parsedTs);
                srv = new DateTime(srv.Year, srv.Month, srv.Day,
                                   srv.Hour, srv.Minute, srv.Second);
                dr["serv_ts"] = srv;

                dt.Rows.Add(dr);
            }
            return dt;
        }
        #endregion

        #region === DB Helper ===
        // itm_info 변경 여부 판단
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
                    return o == null;
                }
            }
        }

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

                    string sql = string.Format(
                        "INSERT INTO public.{0} ({1}) VALUES ({2}) ON CONFLICT DO NOTHING;",
                        tableName, colList, paramList);

                    using (var cmd = new NpgsqlCommand(sql, conn, tx))
                    {
                        // [수정] 파라미터 미리 정의
                        foreach (var c in cols)
                        {
                            var param = new NpgsqlParameter("@" + c, DBNull.Value);
                            // 타입 추론에 맡김 (혹은 DataColumn의 DataType을 NpgsqlDbType으로 매핑)
                            cmd.Parameters.Add(param);
                        }

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
                        catch (Exception ex)
                        {
                            tx.Rollback();
                            SimpleLogger.Error($"DB FAIL ({tableName}) ▶ " + ex.Message);
                            return 0;
                        }
                    }
                }
            }
        }
        #endregion

        #region === Utility ===

        // [수정] WaitForFileReady (공유 읽기/쓰기 모드 확인)
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
                            return true;
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
