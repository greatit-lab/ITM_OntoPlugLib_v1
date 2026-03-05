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
    /*──────────────────────── Logger ────────────────────────*/
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

        // [핵심 해결 1] 메모리 폭증 방지 (최대 동시 작업 3개로 제한)
        // 수백 개의 파일이 몰리거나 서버가 멈춰도 백그라운드 스레드가 무한정 쌓이는 것을 방지합니다.
        private static readonly SemaphoreSlim _uploadLock = new SemaphoreSlim(3, 3);

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
            // [핵심 해결 2] UI 멈춤 방지 (비동기 백그라운드 작업 분리)
            // 작업 완료를 기다리지 않고 바로 리턴하여, 메인 화면(UI)이 절대 멈추지 않게 만듭니다.
            Task.Run(async () =>
            {
                // 메모리 보호를 위해 최대 동시 실행 수 대기
                await _uploadLock.WaitAsync().ConfigureAwait(false);
                try
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

                    // 1. Health Check (UI 멈춤 없이 비동기로 체크 대기)
                    bool isServerHealthy = await CheckServerHealthAsync(currentApiUrl).ConfigureAwait(false);
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

                    // 3. 업로드 및 결과 수신 (UI 멈춤 없이 비동기로 대기)
                    string referenceAddress = await UploadFileAsync(currentApiUrl, filePath, sdwt, eqpid).ConfigureAwait(false);

                    if (!string.IsNullOrEmpty(referenceAddress))
                    {
                        // 4. Full URL 조합 (필요 시) 및 DB 적재
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
                finally
                {
                    // 처리가 끝나면 반드시 락을 해제하여 다음 파일이 처리되도록 함
                    _uploadLock.Release();
                }
            });
        }

        private async Task<bool> CheckServerHealthAsync(string baseUrl)
        {
            try
            {
                // 타임아웃 5초 설정 유지
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
                {
                    // ConfigureAwait(false)를 추가하여 크로스 스레드 데드락 원천 차단
                    var response = await httpClient.GetAsync($"{baseUrl}/api/FileUpload/health", cts.Token).ConfigureAwait(false);
                    return response.IsSuccessStatusCode;
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Health check failed: {ex.Message}");
                return false;
            }
        }

        // JSON 파싱 로직 개선 유지
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

                    // 데드락 방지 ConfigureAwait(false)
                    var response = await httpClient.PostAsync($"{baseUrl}/api/FileUpload/upload", content).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode)
                    {
                        var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
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
