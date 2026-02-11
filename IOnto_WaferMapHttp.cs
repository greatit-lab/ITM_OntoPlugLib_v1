// Library/IOnto_WaferMapHttp.cs
using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ConnectInfo; // DatabaseInfo 사용
using ITM_Agent.Services;
using Npgsql; // NpgsqlConnectionStringBuilder 사용

namespace Onto_WaferMapHttpLib
{
    /* (SimpleLogger 클래스는 기존과 동일) */
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) => _debugEnabled = enable;
        private static readonly object _sync = new object();
        private static readonly string _logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static void Write(string suffix, string msg)
        {
            try
            {
                lock (_sync)
                {
                    Directory.CreateDirectory(_logDir);
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [WaferMapHttp] {msg}{Environment.NewLine}";
                    File.AppendAllText(Path.Combine(_logDir, $"{DateTime.Now:yyyyMMdd}_{suffix}.log"), line, Encoding.UTF8);
                }
            }
            catch { }
        }
        public static void Event(string msg) => Write("event", msg);
        public static void Error(string msg) => Write("error", msg);
        public static void Debug(string msg) { if (_debugEnabled) Write("debug", msg); }
    }

    public interface IOnto_WaferMapHttp
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object settingsPathObj = null, object arg2 = null);
    }

    public class Onto_WaferMapHttp : IOnto_WaferMapHttp
    {
        private static readonly HttpClient httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(300)
        };

        // API 포트 번호
        private const int ApiPort = 8082;

        public string PluginName => "Onto_WaferMapHttp";
        public string DefaultTaskName => "WaferMap";

        /// <summary>
        /// 현재 활성화된 DB 연결 문자열에서 Host IP를 추출하여 API URL을 동적으로 생성합니다.
        /// </summary>
        private string GetDynamicApiUrl()
        {
            try
            {
                // 1. 현재 활성화된(Connection.ini에 설정된) DB 연결 문자열 가져오기
                string connString = DatabaseInfo.CreateDefault().GetConnectionString();

                // 2. 연결 문자열 파싱
                var builder = new NpgsqlConnectionStringBuilder(connString);
                string host = builder.Host; // 현재 DB 서버의 IP

                // 3. API URL 조합
                return $"http://{host}:{ApiPort}";
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Failed to derive API URL from DB Connection: {ex.Message}");
                return $"http://127.0.0.1:{ApiPort}";
            }
        }

        public void ProcessAndUpload(string filePath, object settingsPathObj = null, object arg2 = null)
        {
            SimpleLogger.Event($"ProcessAndUpload ▶ {Path.GetFileName(filePath)}");
            if (!WaitForFileReady(filePath))
            {
                SimpleLogger.Error($"SKIP – File is locked or does not exist: {filePath}");
                return;
            }

            string eqpid = GetEqpidFromSettings(settingsPathObj as string ?? "Settings.ini");
            if (string.IsNullOrEmpty(eqpid))
            {
                SimpleLogger.Error("Eqpid not found. Aborting.");
                return;
            }

            // 동적 URL 생성
            string currentApiUrl = GetDynamicApiUrl();
            SimpleLogger.Debug($"Target API URL: {currentApiUrl}");

            try
            {
                // 1. Health Check
                bool isServerHealthy = CheckServerHealthAsync(currentApiUrl).GetAwaiter().GetResult();
                if (!isServerHealthy)
                {
                    SimpleLogger.Error($"API server health check failed ({currentApiUrl}). Aborting.");
                    return;
                }

                // 2. SDWT 조회
                string sdwt = GetSdwtFromDatabase(eqpid);
                if (string.IsNullOrEmpty(sdwt))
                {
                    SimpleLogger.Error($"SDWT not found for eqpid '{eqpid}'. Aborting.");
                    return;
                }

                // 3. 업로드 및 결과 수신
                string referenceAddress = UploadFileAsync(currentApiUrl, filePath, sdwt, eqpid).GetAwaiter().GetResult();

                if (!string.IsNullOrEmpty(referenceAddress))
                {
                    // 4. Full URL 조합 (필요 시) 및 DB 적재
                    // API가 리턴한 referenceAddress가 이미 상대경로를 포함하므로 그대로 사용하거나 조합
                    // 여기서는 API URL과 조합하여 Full URL을 만듭니다.
                    string fullUri = currentApiUrl + referenceAddress;

                    // 5. DB 적재
                    InsertToDatabase(filePath, eqpid, fullUri);

                    // 6. 삭제
                    TryDeleteLocalFile(filePath);

                    SimpleLogger.Event($"SUCCESS - Uploaded to {currentApiUrl}");
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX: {ex.GetBaseException().Message}");
            }
        }

        private async Task<bool> CheckServerHealthAsync(string baseUrl)
        {
            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
                {
                    var response = await httpClient.GetAsync($"{baseUrl}/api/FileUpload/health", cts.Token);
                    return response.IsSuccessStatusCode;
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Health check failed: {ex.Message}");
                return false;
            }
        }

        // [수정] JSON 파싱 로직 개선 (대소문자 무시 및 로깅 추가)
        private async Task<string> UploadFileAsync(string baseUrl, string filePath, string sdwt, string eqpid)
        {
            try
            {
                using (var content = new MultipartFormDataContent())
                using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    content.Add(new StreamContent(fileStream), "file", Path.GetFileName(filePath));
                    content.Add(new StringContent(sdwt), "sdwt");
                    content.Add(new StringContent(eqpid), "eqpid");

                    var response = await httpClient.PostAsync($"{baseUrl}/api/FileUpload/upload", content);

                    if (response.IsSuccessStatusCode)
                    {
                        var responseString = await response.Content.ReadAsStringAsync();
                        using (var jsonDoc = JsonDocument.Parse(responseString))
                        {
                            // 대소문자 구분 없이 "referenceAddress" 속성 찾기
                            foreach (var prop in jsonDoc.RootElement.EnumerateObject())
                            {
                                if (prop.Name.Equals("referenceAddress", StringComparison.OrdinalIgnoreCase))
                                {
                                    return prop.Value.GetString();
                                }
                            }

                            // 키를 찾지 못한 경우 응답 내용 로깅
                            SimpleLogger.Error($"[Upload] JSON Key 'referenceAddress' not found. Server response: {responseString}");
                            return null;
                        }
                    }
                    else
                    {
                        SimpleLogger.Error($"Upload failed code: {response.StatusCode}");
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Upload Exception: {ex.Message}");
                return null;
            }
        }

        private void InsertToDatabase(string localFilePath, string eqpid, string fileUri)
        {
            string fileName = Path.GetFileName(localFilePath);
            DateTime fileDateTime = ExtractDateTimeFromFileName(fileName);

            var dbInfo = DatabaseInfo.CreateDefault();
            using (var conn = new NpgsqlConnection(dbInfo.GetConnectionString()))
            {
                conn.Open();
                const string sql = @"
                    INSERT INTO public.plg_wf_map 
                        (eqpid, datetime, file_uri, original_filename, serv_ts)
                    VALUES 
                        (@eqpid, @datetime, @file_uri, @original_filename, @serv_ts)
                    ON CONFLICT (eqpid, datetime, original_filename) DO NOTHING;";

                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    DateTime serv_kst = TimeSyncProvider.Instance.ToSynchronizedKst(fileDateTime);
                    serv_kst = new DateTime(serv_kst.Year, serv_kst.Month, serv_kst.Day, serv_kst.Hour, serv_kst.Minute, serv_kst.Second);

                    cmd.Parameters.AddWithValue("@eqpid", eqpid);
                    cmd.Parameters.AddWithValue("@datetime", fileDateTime);
                    cmd.Parameters.AddWithValue("@file_uri", fileUri);
                    cmd.Parameters.AddWithValue("@original_filename", fileName);
                    cmd.Parameters.AddWithValue("@serv_ts", serv_kst);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private string GetSdwtFromDatabase(string eqpid)
        {
            try
            {
                var dbInfo = DatabaseInfo.CreateDefault();
                using (var conn = new NpgsqlConnection(dbInfo.GetConnectionString()))
                {
                    conn.Open();
                    const string sql = "SELECT sdwt FROM public.ref_equipment WHERE eqpid = @eqpid LIMIT 1;";
                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        cmd.Parameters.AddWithValue("@eqpid", eqpid);
                        object result = cmd.ExecuteScalar();
                        return result?.ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Get SDWT failed: {ex.Message}");
                return null;
            }
        }

        private void TryDeleteLocalFile(string filePath)
        {
            try { File.Delete(filePath); } catch { }
        }

        private DateTime ExtractDateTimeFromFileName(string fileName)
        {
            try
            {
                string[] parts = fileName.Split('_');
                if (parts.Length >= 2) return DateTime.ParseExact($"{parts[0]}{parts[1]}", "yyyyMMddHHmmss", null);
            }
            catch { }
            return DateTime.Now;
        }

        private bool WaitForFileReady(string path, int maxRetries = 10, int delayMs = 500)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                if (!File.Exists(path)) return false;
                try { using (File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) return true; }
                catch (IOException) { Thread.Sleep(delayMs); }
            }
            return false;
        }

        private string GetEqpidFromSettings(string iniPath)
        {
            string fullPath = Path.IsPathRooted(iniPath) ? iniPath : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, iniPath);
            if (!File.Exists(fullPath)) return "";
            foreach (var line in File.ReadAllLines(fullPath))
            {
                if (line.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                {
                    var parts = line.Split('=');
                    if (parts.Length > 1) return parts[1].Trim();
                }
            }
            return "";
        }
    }
}
