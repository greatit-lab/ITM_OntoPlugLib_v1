// Library/IOnto_SpectrumData.cs
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ConnectInfo;
using Npgsql;
using NpgsqlTypes; // 파라미터 타입 정의용
using ITM_Agent.Services; // TimeSyncProvider 사용

namespace Onto_SpectrumDataLib
{
    /* 로거 (기존 동일) */
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) => _debugEnabled = enable;
        private static readonly object _sync = new object();
        private static readonly string _logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

        public static void Event(string msg) => Write("event", msg);
        public static void Error(string msg) => Write("error", msg);
        public static void Debug(string msg) { if (_debugEnabled) Write("debug", msg); }

        private static void Write(string suffix, string msg)
        {
            try
            {
                lock (_sync)
                {
                    Directory.CreateDirectory(_logDir);
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Spectrum] {msg}{Environment.NewLine}";
                    File.AppendAllText(Path.Combine(_logDir, $"{DateTime.Now:yyyyMMdd}_{suffix}.log"), line, Encoding.UTF8);
                }
            }
            catch { }
        }
    }

    public interface IOnto_SpectrumData
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object settingsPathObj = null, object arg2 = null);
    }

    public class Onto_SpectrumData : IOnto_SpectrumData
    {
        public string PluginName => "Onto_SpectrumData";
        public string DefaultTaskName => "Spectrum";
        public string DefaultFileFilter => "*Exp.dat;*Gen.dat";
        public bool RequiresOverrideNames => true;

        // 배치 처리용 큐 및 타이머
        private static readonly ConcurrentQueue<BatchItem> _batchQueue = new ConcurrentQueue<BatchItem>();
        private static readonly object _flushLock = new object();
        private static Timer _flushTimer;
        private const int BATCH_SIZE = 50;
        private const int FLUSH_INTERVAL_MS = 3000;

        private class BatchItem
        {
            public string FilePath;
            public FileMeta Meta;
            public SpectrumData Data;
        }

        static Onto_SpectrumData()
        {
            _flushTimer = new Timer(_ => FlushQueue(), null, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS);
            try { Encoding.RegisterProvider(CodePagesEncodingProvider.Instance); } catch { }
        }

        public Onto_SpectrumData() { }

        public void ProcessAndUpload(string filePath, object settingsPathObj = null, object arg2 = null)
        {
            if (!WaitForFileReady(filePath))
            {
                // [수정] 파일 잠김/미존재는 Debug 로그로 전환
                SimpleLogger.Debug($"File locked or not found (Skipping): {filePath}");
                return;
            }

            try
            {
                // [개선] 31PT 문제 및 분리된 LotID 문제 해결 로직 적용
                var meta = ParseFileName(filePath);
                if (meta == null)
                {
                    // 파싱 실패 시 로그 남기고 종료 (빈 리턴 방지)
                    SimpleLogger.Debug($"Filename parse failed: {Path.GetFileName(filePath)}");
                    return;
                }

                meta.EqpId = GetEqpidFromSettings(settingsPathObj as string);

                var data = ParseFileContent(filePath);
                if (data == null || data.Wavelengths.Count == 0) return;

                _batchQueue.Enqueue(new BatchItem
                {
                    FilePath = filePath,
                    Meta = meta,
                    Data = data
                });

                if (_batchQueue.Count >= BATCH_SIZE)
                {
                    Task.Run(() => FlushQueue());
                }
            }
            catch (IOException ioEx)
            {
                // [수정] IO 예외는 Debug 로그로 처리
                SimpleLogger.Debug($"Process Locked (IOException): {ioEx.Message}");
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Process Error: {ex.Message}");
            }
        }

        private static void FlushQueue()
        {
            lock (_flushLock)
            {
                if (_batchQueue.IsEmpty) return;

                var itemsToProcess = new List<BatchItem>();
                while (_batchQueue.TryDequeue(out var item))
                {
                    itemsToProcess.Add(item);
                }

                if (itemsToProcess.Count == 0) return;

                bool success = BulkInsert(itemsToProcess);

                if (success)
                {
                    int deletedCount = 0;
                    foreach (var item in itemsToProcess)
                    {
                        try
                        {
                            File.Delete(item.FilePath);
                            deletedCount++;
                        }
                        catch { }
                    }
                    SimpleLogger.Event($"Batch Success: {itemsToProcess.Count} items uploaded, {deletedCount} files deleted.");
                }
                else
                {
                    SimpleLogger.Error($"Batch Insert Failed. {itemsToProcess.Count} files remain.");
                }
            }
        }

        private static bool BulkInsert(List<BatchItem> items)
        {
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();

            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (var tx = conn.BeginTransaction())
                    {
                        // [수정] serv_ts 컬럼 포함
                        const string sql = @"
                            INSERT INTO public.plg_onto_spectrum
                            (
                                eqpid, ts, serv_ts, 
                                lotid, waferid, point, 
                                class, type, 
                                angle, val_summary, wavelengths, ""values""
                            )
                            VALUES
                            (
                                @eqpid, @ts, @serv_ts,
                                @lotid, @waferid, @point,
                                @class, @type,
                                @angle, @val_summary, @wavelengths, @values
                            )
                            ON CONFLICT (eqpid, ts, point, class, type)
                            DO UPDATE SET
                                angle = EXCLUDED.angle,
                                val_summary = EXCLUDED.val_summary,
                                wavelengths = EXCLUDED.wavelengths,
                                ""values"" = EXCLUDED.""values"",
                                serv_ts = EXCLUDED.serv_ts;
                        ";

                        using (var cmd = new NpgsqlCommand(sql, conn, tx))
                        {
                            cmd.Parameters.Add(new NpgsqlParameter("@eqpid", NpgsqlDbType.Varchar));
                            cmd.Parameters.Add(new NpgsqlParameter("@ts", NpgsqlDbType.Timestamp));
                            cmd.Parameters.Add(new NpgsqlParameter("@serv_ts", NpgsqlDbType.Timestamp));
                            cmd.Parameters.Add(new NpgsqlParameter("@lotid", NpgsqlDbType.Varchar));
                            cmd.Parameters.Add(new NpgsqlParameter("@waferid", NpgsqlDbType.Varchar));
                            cmd.Parameters.Add(new NpgsqlParameter("@point", NpgsqlDbType.Integer));
                            cmd.Parameters.Add(new NpgsqlParameter("@class", NpgsqlDbType.Varchar));
                            cmd.Parameters.Add(new NpgsqlParameter("@type", NpgsqlDbType.Varchar));
                            cmd.Parameters.Add(new NpgsqlParameter("@angle", NpgsqlDbType.Real));
                            cmd.Parameters.Add(new NpgsqlParameter("@val_summary", NpgsqlDbType.Real));
                            cmd.Parameters.Add(new NpgsqlParameter("@wavelengths", NpgsqlDbType.Array | NpgsqlDbType.Real));
                            cmd.Parameters.Add(new NpgsqlParameter("@values", NpgsqlDbType.Array | NpgsqlDbType.Real));

                            foreach (var item in items)
                            {
                                // [수정] 서버 시간 보정 및 초 단위 절삭 (YYYY-MM-DD HH:MM:SS)
                                DateTime rawKst = TimeSyncProvider.Instance.ToSynchronizedKst(item.Meta.MeasureTs);
                                DateTime truncatedKst = new DateTime(rawKst.Year, rawKst.Month, rawKst.Day, rawKst.Hour, rawKst.Minute, rawKst.Second);

                                cmd.Parameters["@eqpid"].Value = item.Meta.EqpId;
                                cmd.Parameters["@ts"].Value = item.Meta.MeasureTs;
                                cmd.Parameters["@serv_ts"].Value = truncatedKst; // 절삭된 시간
                                cmd.Parameters["@lotid"].Value = item.Meta.LotId;
                                cmd.Parameters["@waferid"].Value = item.Meta.WaferId;
                                cmd.Parameters["@point"].Value = item.Meta.PointNo;
                                cmd.Parameters["@class"].Value = item.Meta.DataClass;
                                cmd.Parameters["@type"].Value = item.Data.PolType ?? (object)DBNull.Value;
                                cmd.Parameters["@angle"].Value = item.Data.AngleVal;
                                cmd.Parameters["@val_summary"].Value = item.Data.ValSummary.HasValue ? (object)item.Data.ValSummary.Value : DBNull.Value;
                                cmd.Parameters["@wavelengths"].Value = item.Data.Wavelengths.ToArray();
                                cmd.Parameters["@values"].Value = item.Data.Values.ToArray();

                                cmd.ExecuteNonQuery();
                            }
                        }
                        tx.Commit();
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Bulk Insert Error: {ex.Message}");
                return false;
            }
        }

        // --- 내부 클래스 ---
        private class FileMeta { public string EqpId; public DateTime MeasureTs; public string LotId; public string WaferId; public int PointNo; public string DataClass; }
        private class SpectrumData { public string PolType; public float AngleVal; public float? ValSummary; public List<float> Wavelengths = new List<float>(); public List<float> Values = new List<float>(); }

        // --- [핵심 수정] 파일명 파싱 로직 ---
        private FileMeta ParseFileName(string filePath)
        {
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string[] parts = fileName.Split('_');

            // 최소 길이 체크 (날짜2 + EQP1 + Lot1 + Wafer1 = 최소 5개)
            if (parts.Length < 5) return null;

            var meta = new FileMeta();

            try
            {
                // 1. 날짜/시간 (인덱스 0, 1은 항상 고정)
                string dateStr = parts[0] + parts[1];
                if (!DateTime.TryParseExact(dateStr, "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.None, out meta.MeasureTs))
                {
                    meta.MeasureTs = DateTime.Now;
                }

                // 2. WaferID 패턴(W+숫자)의 위치 찾기
                int waferIndex = -1;
                for (int i = 0; i < parts.Length; i++)
                {
                    // "W"를 포함하고 뒤에 숫자가 오는지 확인 (가장 확실한 기준점)
                    var match = Regex.Match(parts[i], @"W(\d+)");
                    if (match.Success)
                    {
                        waferIndex = i;
                        meta.WaferId = match.Groups[1].Value; // 숫자만 추출 (예: 13)
                        break;
                    }
                }

                if (waferIndex == -1)
                {
                    // WaferID 패턴을 못 찾으면 기존 방식(인덱스 기반)이나 Unknown 처리
                    // 여기서는 안전하게 리턴
                    SimpleLogger.Debug($"WaferID pattern (W#) NOT found: {fileName}");
                    return null;
                }

                // 3. LotID 추출 (WaferID 바로 앞의 1~2개 덩어리만 확인)
                // "31PT" 같은게 앞에 있어도, WaferID 바로 앞만 보기 때문에 무시됨

                if (waferIndex >= 2)
                {
                    string prev1 = parts[waferIndex - 1]; // Wafer 바로 앞 (예: "1" 또는 "AAA001" 또는 "LOT123")

                    // [판단 로직] 바로 앞이 "숫자"로만 구성되어 있다면 -> 분리된 LotID의 뒷부분(Suffix)임
                    if (Regex.IsMatch(prev1, @"^\d+$"))
                    {
                        // 분리된 경우: 그 앞부분(prev2)과 합침
                        // 예: ..._A1AA00_1_C1W13... -> "A1AA00.1"
                        // 예: ..._31PT_AAA001_1_C3W12... -> "AAA001.1" (31PT는 무시됨)
                        string prev2 = parts[waferIndex - 2];
                        meta.LotId = $"{prev2}.{prev1}";
                    }
                    else
                    {
                        // 분리되지 않은 경우: 바로 앞부분이 전체 LotID임
                        // 예: ..._LOT123_C1W13... -> "LOT123"
                        meta.LotId = prev1;
                    }
                }
                else if (waferIndex == 1)
                {
                    // 구조상 거의 없겠지만 예외 처리
                    meta.LotId = parts[0];
                }
                else
                {
                    meta.LotId = "Unknown";
                }

                // 4. PointNo 및 DataClass 추출 (파일명 맨 뒤)
                string lastPart = parts[parts.Length - 1];
                if (lastPart.EndsWith("Exp", StringComparison.OrdinalIgnoreCase))
                {
                    meta.DataClass = "EXP";
                    string numStr = Regex.Replace(lastPart, "[^0-9]", "");
                    int.TryParse(numStr, out meta.PointNo);
                }
                else if (lastPart.EndsWith("Gen", StringComparison.OrdinalIgnoreCase))
                {
                    meta.DataClass = "GEN";
                    string numStr = Regex.Replace(lastPart, "[^0-9]", "");
                    int.TryParse(numStr, out meta.PointNo);
                }
                else
                {
                    meta.DataClass = "UNK";
                    meta.PointNo = 0;
                }

                return meta;
            }
            catch
            {
                return null;
            }
        }

        private SpectrumData ParseFileContent(string filePath)
        {
            var result = new SpectrumData();
            const float TARGET_WAVELENGTH = 633.0f;
            float minDiff = float.MaxValue;
            bool angleFound = false;

            using (var sr = new StreamReader(filePath, Encoding.Default))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    if (line.StartsWith("sR", StringComparison.OrdinalIgnoreCase) ||
                        line.StartsWith("uR", StringComparison.OrdinalIgnoreCase))
                    {
                        var tokens = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if (tokens.Length >= 4)
                        {
                            result.PolType = tokens[0];
                            if (float.TryParse(tokens[1], out float nm) && float.TryParse(tokens[3], out float val))
                            {
                                if (!angleFound && float.TryParse(tokens[2], out float ang))
                                {
                                    result.AngleVal = ang;
                                    angleFound = true;
                                }

                                result.Wavelengths.Add(nm);
                                result.Values.Add(val);

                                float diff = Math.Abs(nm - TARGET_WAVELENGTH);
                                if (diff < minDiff)
                                {
                                    minDiff = diff;
                                    result.ValSummary = val;
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }

        private string GetEqpidFromSettings(string iniPath)
        {
            string path = iniPath ?? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Settings.ini");
            if (!File.Exists(path)) return "UNKNOWN";
            try
            {
                foreach (var line in File.ReadAllLines(path))
                    if (line.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                        return line.Split('=')[1].Trim();
            }
            catch { }
            return "UNKNOWN";
        }

        private bool WaitForFileReady(string path, int maxRetries = 5, int delayMs = 500)
        {
            for (int i = 0; i < maxRetries; i++) { try { using (File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) return true; } catch (IOException) { Thread.Sleep(delayMs); } }
            return false;
        }
    }
}
