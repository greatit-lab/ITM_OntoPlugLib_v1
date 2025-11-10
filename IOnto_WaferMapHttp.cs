// Library/IOnto_WaferMapHttp.cs
using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json; // ★ NuGet 패키지 관리자에서 System.Text.Json 설치 필요
using System.Threading;
using System.Threading.Tasks;
using ConnectInfo;
using ITM_Agent.Services;
using Npgsql;

namespace Onto_WaferMapHttpLib
{
    /* ──────────────────────── Logger ──────────────────────── */
    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) => _debugEnabled = enable;
        private static readonly object _sync = new object();
        private static readonly string _logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");
        private static void Write(string suffix, string msg)
        {
            lock (_sync)
            {
                try
                {
                    Directory.CreateDirectory(_logDir);
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [WaferMapHttp] {msg}{Environment.NewLine}";
                    File.AppendAllText(Path.Combine(_logDir, $"{DateTime.Now:yyyyMMdd}_{suffix}.log"), line, Encoding.UTF8);
                }
                catch { /* 로깅 실패는 무시 */ }
            }
        }
        public static void Event(string msg) => Write("event", msg);
        public static void Error(string msg) => Write("error", msg);
        public static void Debug(string msg) { if (_debugEnabled) Write("debug", msg); }
    }

    /* ─────────────────── 인터페이스 및 클래스 ─────────────────── */
    public interface IOnto_WaferMapHttp
    {
        string PluginName { get; }
        void ProcessAndUpload(string filePath, object settingsPathObj = null, object arg2 = null);
    }

    public class Onto_WaferMapHttp : IOnto_WaferMapHttp
    {
        // ▼▼▼ [수정] HttpClient 타임아웃 기본값을 5분으로 설정 ▼▼▼
        private static readonly HttpClient httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(300)
        };

        // ★★★ 직접 만드신 API 서버의 주소를 여기에 입력하세요! ★★★
        private const string ApiBaseUrl = "http://192.168.0.10:8080"; // 예: http://서버IP:포트

        public string PluginName => "Onto_WaferMapHttp";

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
                SimpleLogger.Error("Eqpid not found in Settings.ini. Aborting process.");
                return;
            }

            try
            {
                // 1. (가장 먼저) API 서버가 정상적으로 응답하는지 확인합니다.
                bool isServerHealthy = CheckServerHealthAsync().GetAwaiter().GetResult();
                if (!isServerHealthy)
                {
                    SimpleLogger.Error($"API server health check failed. Aborting upload for {filePath}");
                    return; // 서버에 접속할 수 없으면 작업을 중단합니다.
                }

                // 2. DB에서 sdwt 값을 가져옵니다.
                string sdwt = GetSdwtFromDatabase(eqpid);
                if (string.IsNullOrEmpty(sdwt))
                {
                    SimpleLogger.Error($"SDWT not found for eqpid '{eqpid}'. Aborting upload.");
                    return;
                }

                // 3. 파일과 함께 sdwt, eqpid를 업로드합니다.
                string referenceAddress = UploadFileAsync(filePath, sdwt, eqpid).GetAwaiter().GetResult();

                if (!string.IsNullOrEmpty(referenceAddress))
                {
                    // 4. API 서버의 기본 주소와 참조 주소를 조합하여 완전한 URL을 만듭니다.
                    //    API가 반환하는 주소가 /로 시작하므로 그대로 붙입니다.
                    string fullUri = ApiBaseUrl + referenceAddress;

                    // 5. DB에 완전한 URL을 저장하고 원본 파일을 삭제합니다.
                    InsertToDatabase(filePath, eqpid, fullUri);
                    TryDeleteLocalFile(filePath);

                    SimpleLogger.Event($"SUCCESS - Uploaded {Path.GetFileName(filePath)} and created DB record.");
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Unhandled EX in ProcessAndUpload for {filePath} ▶ {ex.GetBaseException().Message}");
            }
        }

        private async Task<bool> CheckServerHealthAsync()
        {
            try
            {
                // 타임아웃을 10초로 설정하여 응답이 없을 때 오래 기다리지 않도록 합니다.
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    var response = await httpClient.GetAsync($"{ApiBaseUrl}/api/FileUpload/health", cts.Token);
                    if (response.IsSuccessStatusCode)
                    {
                        SimpleLogger.Debug("API server health check successful.");
                        return true;
                    }
                    else
                    {
                        SimpleLogger.Error($"API server health check failed with status code: {response.StatusCode}");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Failed to connect to API server for health check: {ex.Message}");
                return false;
            }
        }

        private async Task<string> UploadFileAsync(string filePath, string sdwt, string eqpid)
        {
            try
            {
                using (var content = new MultipartFormDataContent())
                using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    // HTTP 요청 본문에 파일, sdwt, eqpid를 각각 추가합니다.
                    content.Add(new StreamContent(fileStream), "file", Path.GetFileName(filePath));
                    content.Add(new StringContent(sdwt), "sdwt");
                    content.Add(new StringContent(eqpid), "eqpid");

                    // API 서버의 업로드 주소로 POST 요청을 보냅니다.
                    var response = await httpClient.PostAsync($"{ApiBaseUrl}/api/FileUpload/upload", content);

                    if (response.IsSuccessStatusCode)
                    {
                        // 성공 시, 응답 본문(JSON)에서 참조 주소를 추출합니다.
                        var responseString = await response.Content.ReadAsStringAsync();
                        using (var jsonDoc = JsonDocument.Parse(responseString))
                        {
                            return jsonDoc.RootElement.GetProperty("referenceAddress").GetString();
                        }
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        SimpleLogger.Error($"File upload failed with status code: {response.StatusCode}. Details: {errorContent}");
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"An exception occurred during file upload: {ex.Message}");
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

                // `file_uri` 컬럼에 FTP 주소 대신 API가 반환한 완전한 URL을 저장합니다.
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
                        if (result != null && result != DBNull.Value)
                        {
                            SimpleLogger.Debug($"Found sdwt '{result}' for eqpid '{eqpid}'.");
                            return result.ToString();
                        }
                        else
                        {
                            SimpleLogger.Error($"SDWT not found in ref_equipment for eqpid: {eqpid}");
                            return null;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Failed to get SDWT from DB for eqpid {eqpid}. EX: {ex.Message}");
                return null;
            }
        }

        #region Helper Methods

        private void TryDeleteLocalFile(string filePath)
        {
            try
            {
                File.Delete(filePath);
                SimpleLogger.Debug($"Local file deleted: {filePath}");
            }
            catch (Exception ex)
            {
                SimpleLogger.Error($"Failed to delete local file {filePath}: {ex.Message}");
            }
        }

        private DateTime ExtractDateTimeFromFileName(string fileName)
        {
            try
            {
                string[] parts = fileName.Split('_');
                if (parts.Length >= 2)
                {
                    return DateTime.ParseExact($"{parts[0]}{parts[1]}", "yyyyMMddHHmmss", null);
                }
            }
            catch { /* 파싱 실패 시 현재 시간으로 대체 */ }
            return DateTime.Now;
        }

        private bool WaitForFileReady(string path, int maxRetries = 10, int delayMs = 500)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                if (!File.Exists(path)) return false;
                try
                {
                    using (File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        return true;
                    }
                }
                catch (IOException) { Thread.Sleep(delayMs); }
            }
            return false;
        }

        private string GetEqpidFromSettings(string iniPath)
        {
            // ini 파일이 상대 경로일 경우를 대비해 BaseDirectory와 결합
            string fullPath = Path.IsPathRooted(iniPath) ? iniPath : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, iniPath);

            if (!File.Exists(fullPath)) return "";
            foreach (var line in File.ReadAllLines(fullPath))
            {
                if (line.Trim().StartsWith("Eqpid", StringComparison.OrdinalIgnoreCase))
                {
                    int idx = line.IndexOf('=');
                    if (idx > 0) return line.Substring(idx + 1).Trim();
                }
            }
            return "";
        }
        #endregion
    }
}
