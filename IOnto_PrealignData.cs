// Library/IOnto_PrealignData.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO; // ★
using System.Linq;
using System.Reflection;
using System.Text; // ★
using System.Text.RegularExpressions;
using System.Threading;
using ConnectInfo;
using Npgsql;
using ITM_Agent.Services; // ★ using 구문 확인

namespace Onto_PrealignDataLib
{
    /*──────────────────────── Logger ────────────────────────*/
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) { _debugEnabled = enable; }

        private static readonly object _sync = new object();
        private static readonly string _dir = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static string PathOf(string sfx) => System.IO.Path.Combine(_dir, $"{DateTime.Now:yyyyMMdd}_{sfx}.log");

        private static void Write(string s, string m)
        {
            lock (_sync)
            {
                System.IO.Directory.CreateDirectory(_dir);
                string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Prealign] {m}{Environment.NewLine}";
                try { System.IO.File.AppendAllText(PathOf(s), line, System.Text.Encoding.UTF8); }
                catch { /* 로깅 실패 무시 */ }
            }
        }

        public static void Event(string m) { Write("event", m); }
        public static void Error(string m) { Write("error", m); }
        public static void Debug(string m)
        {
            if (_debugEnabled) Write("debug", m);
        }
    }

    /*──────────────────────── Interface ─────────────────────*/
    public interface IOnto_PrealignData
    {
        string PluginName { get; }
        // (string, object, object) 표준 시그니처
        void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null);
        // [삭제] Watcher 로직은 ucUploadPanel이 담당
        // void StartWatch(string folderPath);
        // void StopWatch();
    }

    /*──────────────────────── Implementation ────────────────*/
    public class Onto_PrealignData : IOnto_PrealignData
    {
        /* 상태 · 상수 */
        // ★ [핵심] 증분 처리를 위한 마지막 파일 크기(Offset) 저장소
        private static readonly Dictionary<string, long> _lastLen =
            new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

        // [삭제] Watcher 관련 필드 제거
        // private FileSystemWatcher _fw;
        // private DateTime _lastEvt = DateTime.MinValue;

        private readonly string _pluginName;
        public string PluginName => _pluginName;

        // [추가] Req 3. 메타데이터 속성
        public string DefaultTaskName => "PreAlign";
        public string DefaultFileFilter => "PreAlignLog.dat";

        /* ... (생성자 및 정적 생성자 기존과 동일) ... */
        static Onto_PrealignData()
        {
            #if NETCOREAPP || NET5_0_OR_GREATER
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            #endif
        }
        public Onto_PrealignData()
        {
            _pluginName = "Onto_PrealignData"; // 단순화
        }

        /*──────────────── Folder Watch (삭제) ─────────────────────*/
        // [삭제] StartWatch, StopWatch, OnChanged 메서드

        /*──────────────── 증분 처리 (ProcessAndUpload로 통합) ───*/

        // [수정] ucUploadPanel (탭 2)에서 호출하는 증분 처리 로직
        public void ProcessAndUpload(string filePath, object arg1 = null, object arg2 = null)
        {
            SimpleLogger.Event("Process (Incremental) ▶ " + filePath);
            string eqpid = GetEqpid(arg1 as string ?? "Settings.ini");
            
            long prevLen = 0;
            long currLen = 0;
            
            // [핵심 수정] CS0165 오류 해결: 변수 선언 시 즉시 초기화
            string addedText = ""; 

            int maxRetries = 5;
            int delayMs = 300;

            try
            {
                lock (_lastLen)
                {
                    _lastLen.TryGetValue(filePath, out prevLen);
                }

                // ... (파일 접근 재시도 for 루프) ...
                for (int i = 0; i < maxRetries; i++)
                {
                    try
                    {
                        using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                        {
                            currLen = fs.Length;

                            if (currLen == prevLen && prevLen > 0)
                            {
                                SimpleLogger.Debug("File length unchanged, skipping: " + filePath);
                                return; 
                            }

                            if (currLen < prevLen)
                            {
                                SimpleLogger.Event("File truncated. Resetting offset: " + filePath);
                                prevLen = 0; 
                            }

                            fs.Seek(prevLen, SeekOrigin.Begin); 
                            using (var sr = new StreamReader(fs, Encoding.GetEncoding(949)))
                            {
                                addedText = sr.ReadToEnd(); // 여기서 값이 할당됨
                            }
                        }
                        
                        break; // 성공 시 루프 탈출
                    }
                    catch (IOException ioEx) when (i < maxRetries - 1)
                    {
                        SimpleLogger.Debug($"[Prealign] IO Exception attempt {i + 1} (retrying): {ioEx.Message}");
                        Thread.Sleep(delayMs); 
                        // addedText가 할당되지 않은 채로 루프가 계속될 수 있음 (이것이 오류 원인)
                    }
                    catch (IOException ioEx) when (i == maxRetries - 1)
                    {
                        SimpleLogger.Error($"IO Exception during processing {filePath} (retries failed): {ioEx.Message}");
                        return; 
                    }
                } // 재시도 루프 종료

                // (이제 addedText는 "" 또는 읽어온 값으로 무조건 할당되어 있음)
                var rows = new List<Tuple<decimal, decimal, decimal, DateTime>>();
                var rex = new Regex(
                    @"Xmm\s*([-\d.]+)\s*Ymm\s*([-\d.]+)\s*Notch\s*([-\d.]+)\s*Time\s*([\d\-:\s]+)",
                    RegexOptions.IgnoreCase);
                
                // CS0165 오류가 해결됨
                foreach (Match m in rex.Matches(addedText)) 
                {
                    // ... (이하 파싱 및 DB 적재 로직 동일) ...
                }

                if (rows.Count > 0)
                {
                    InsertRows(rows, eqpid); // DB 업로드
                }
                else
                {
                    SimpleLogger.Debug("No valid new rows found in incremental text.");
                }

                lock (_lastLen)
                {
                    _lastLen[filePath] = currLen;
                }
            }
            catch (FileNotFoundException)
            {
                SimpleLogger.Debug("File not found (maybe deleted): " + filePath);
                lock(_lastLen) { _lastLen.Remove(filePath); }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX in ProcessAndUpload for {filePath} ▶ {ex.GetBaseException().Message}");
            }
        }
        
        // [삭제] ProcessCore, ProcessIncremental (ProcessAndUpload로 통합됨)
        // [삭제] WaitReady (대체됨)

        /*──────────────── 행 → DB 삽입 공통 ────────────────*/
        private void InsertRows(List<Tuple<decimal, decimal, decimal, DateTime>> rows, string eqpid)
        {
            rows.Sort((a, b) => a.Item4.CompareTo(b.Item4));

            var dt = new DataTable();
            dt.Columns.Add("eqpid", typeof(string));
            dt.Columns.Add("datetime", typeof(DateTime));
            dt.Columns.Add("xmm", typeof(decimal));
            dt.Columns.Add("ymm", typeof(decimal));
            dt.Columns.Add("notch", typeof(decimal));
            dt.Columns.Add("serv_ts", typeof(DateTime));

            foreach (var r in rows)
            {
                /* ★★★ 핵심 수정 ★★★ */
                // TimeSyncProvider를 통해 오차 보정 및 KST 변환
                DateTime serv_kst = TimeSyncProvider.Instance.ToSynchronizedKst(r.Item4);
                serv_kst = serv_kst.AddTicks(-(serv_kst.Ticks % TimeSpan.TicksPerSecond));

                dt.Rows.Add(eqpid, r.Item4, r.Item1, r.Item2, r.Item3, serv_kst);
            }

            Upload(dt);
            SimpleLogger.Event($"rows={dt.Rows.Count} uploaded successfully.");
        }

        /*──────────────── DB Upload ───────────────────────*/
        private void Upload(DataTable dt)
        {
            // [수정] 30만 건의 데이터를 고속 처리하기 위해 'INSERT 루프'나 'BinaryImport' 대신
            // Npgsql의 'Batch Update' (Unnest) 방식을 사용합니다.
            // 이 방식은 단 한 번의 DB 라운드트립으로 모든 데이터를 전송합니다.

            string cs = DatabaseInfo.CreateDefault().GetConnectionString();
            try
            {
                using (var conn = new NpgsqlConnection(cs))
                {
                    conn.Open();

                    // 1. 모든 컬럼 데이터를 C# 배열로 변환합니다.
                    var eqpids = new List<string>(dt.Rows.Count);
                    var datetimes = new List<DateTime>(dt.Rows.Count);
                    var xmms = new List<decimal>(dt.Rows.Count);
                    var ymms = new List<decimal>(dt.Rows.Count);
                    var notches = new List<decimal>(dt.Rows.Count);
                    var serv_tss = new List<DateTime>(dt.Rows.Count);

                    foreach (DataRow row in dt.Rows)
                    {
                        // InsertRows 메서드에서 이미 타입을 보장했으므로 DBNull 체크 단순화
                        eqpids.Add(row["eqpid"] as string); // eqpid는 null일 수 있음
                        datetimes.Add((DateTime)row["datetime"]);
                        xmms.Add((decimal)row["xmm"]);
                        ymms.Add((decimal)row["ymm"]);
                        notches.Add((decimal)row["notch"]);
                        serv_tss.Add((DateTime)row["serv_ts"]);
                    }

                    // 2. UNNEST를 사용하여 C# 배열을 PostgreSQL 배열 파라미터로 매핑
                    const string sql = @"
                        INSERT INTO public.plg_prealign 
                            (eqpid, datetime, xmm, ymm, notch, serv_ts)
                        SELECT
                            u.eqpid, u.datetime, u.xmm, u.ymm, u.notch, u.serv_ts
                        FROM
                            unnest(
                                @eqpids, 
                                @datetimes, 
                                @xmms, 
                                @ymms, 
                                @notches, 
                                @serv_tss
                            ) AS u(eqpid, datetime, xmm, ymm, notch, serv_ts)
                        ON CONFLICT (eqpid, datetime) DO NOTHING;";

                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        // 3. 파라미터에 배열을 통째로 바인딩
                        cmd.Parameters.AddWithValue("eqpids", eqpids);
                        cmd.Parameters.AddWithValue("datetimes", datetimes);
                        cmd.Parameters.AddWithValue("xmms", xmms);
                        cmd.Parameters.AddWithValue("ymms", ymms);
                        cmd.Parameters.AddWithValue("notches", notches);
                        cmd.Parameters.AddWithValue("serv_tss", serv_tss);

                        // 4. 단 한 번의 실행으로 모든 데이터 전송
                        int affected = cmd.ExecuteNonQuery();
                        SimpleLogger.Debug($"DB Batch OK ▶ Total processed={dt.Rows.Count}, Inserted={affected}");
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null)
                {
                    SimpleLogger.Error($"DB Upload failed: {ex.Message} (Inner: {ex.InnerException.Message})");
                }
                else
                {
                    SimpleLogger.Error("DB Upload failed: " + ex.Message);
                }
            }
        }

        /*──────────────── Utilities ───────────────────────*/
        
        // [삭제] ReadAllText (대체됨)

        private string GetEqpid(string ini)
        {
            string iniPath = Path.IsPathRooted(ini) 
                ? ini 
                : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ini);

            if (!File.Exists(iniPath)) return string.Empty;
            try // [추가] 파일 접근 예외 처리
            {
                foreach (string ln in File.ReadLines(iniPath))
                {
                    if (ln.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = ln.IndexOf('=');
                        if (idx > 0) return ln.Substring(idx + 1).Trim();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("GetEqpid failed: " + ex.Message);
            }
            return string.Empty;
        }
    }
}
