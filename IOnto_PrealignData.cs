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
            string addedText;

            try
            {
                // --- [개선] 증분 처리를 위한 길이 확인 및 파일 읽기 ---
                lock (_lastLen)
                {
                    _lastLen.TryGetValue(filePath, out prevLen);
                }

                // ★ [개선] FileShare.ReadWrite로 증분 읽기 (공유 위반 해결)
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

                    fs.Seek(prevLen, SeekOrigin.Begin); // ★ 마지막 위치로 이동
                    using (var sr = new StreamReader(fs, Encoding.GetEncoding(949)))
                    {
                        addedText = sr.ReadToEnd(); // ★ 새로 추가된 텍스트만 읽음
                    }
                } // FileStream 닫기 (잠금 해제)

                // --- [개선] ProcessFile 로직을 여기로 통합 ---
                var rows = new List<Tuple<decimal, decimal, decimal, DateTime>>();
                var rex = new Regex(
                    @"Xmm\s*([-\d.]+)\s*Ymm\s*([-\d.]+)\s*Notch\s*([-\d.]+)\s*Time\s*([\d\-:\s]+)",
                    RegexOptions.IgnoreCase);
                
                // [수정] addedText만 파싱
                foreach (Match m in rex.Matches(addedText))
                {
                    DateTime ts;
                    bool ok = DateTime.TryParseExact(
                                 m.Groups[4].Value.Trim(),
                                 new[] { "MM-dd-yy HH:mm:ss", "M-d-yy HH:mm:ss" },
                                 CultureInfo.InvariantCulture,
                                 DateTimeStyles.None,
                                 out ts) ||
                              DateTime.TryParse(m.Groups[4].Value.Trim(), out ts);
                    if (!ok) continue;

                    decimal x, y, n;
                    if (decimal.TryParse(m.Groups[1].Value, out x) &&
                        decimal.TryParse(m.Groups[2].Value, out y) &&
                        decimal.TryParse(m.Groups[3].Value, out n))
                    {
                        rows.Add(Tuple.Create(x, y, n, ts));
                    }
                }

                if (rows.Count > 0)
                {
                    InsertRows(rows, eqpid); // DB 업로드
                }
                else
                {
                    SimpleLogger.Debug("No valid new rows found in incremental text.");
                }

                // --- [개선] 현재 파일 크기를 _lastLen에 갱신 ---
                lock (_lastLen)
                {
                    _lastLen[filePath] = currLen;
                }

                // --- [중요] 원본 파일이므로 File.Delete(filePath) 로직은 없습니다. ---
            }
            catch (FileNotFoundException)
            {
                SimpleLogger.Debug("File not found (maybe deleted): " + filePath);
                lock(_lastLen) { _lastLen.Remove(filePath); }
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
            string cs = DatabaseInfo.CreateDefault().GetConnectionString();
            try // [추가] DB 예외 처리
            {
                using (var conn = new NpgsqlConnection(cs))
                {
                    conn.Open();
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.Transaction = tx;
                        string cols = string.Join(",",
                            dt.Columns.Cast<DataColumn>().Select(c => "\"" + c.ColumnName + "\""));
                        string prm = string.Join(",",
                            dt.Columns.Cast<DataColumn>().Select(c => "@" + c.ColumnName));

                        // [수정] 테이블명: public.prealign → public.plg_prealign
                        cmd.CommandText =
                            $"INSERT INTO public.plg_prealign ({cols}) VALUES ({prm}) " +   // [수정]
                            "ON CONFLICT (eqpid, datetime) DO NOTHING;";

                        foreach (DataColumn c in dt.Columns)
                            cmd.Parameters.Add(new NpgsqlParameter("@" + c.ColumnName, DbType.Object));

                        foreach (DataRow r in dt.Rows)
                        {
                            foreach (DataColumn c in dt.Columns)
                                cmd.Parameters["@" + c.ColumnName].Value = r[c] ?? DBNull.Value;
                            cmd.ExecuteNonQuery();
                        }
                        tx.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error("DB Upload failed: " + ex.Message);
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
