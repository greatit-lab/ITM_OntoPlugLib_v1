// ConnectInfo/DatabaseInfo.cs
using System;
using Npgsql;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Security.Cryptography; // (AES, SHA256 사용을 위해 필수)
using Newtonsoft.Json; // (FTP 정보 JSON 역직렬화)

namespace ConnectInfo
{
    // (FTP 정보 역직렬화를 위한 내부 클래스)
    internal class FtpConfig
    {
        public string Host { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }

    public sealed class DatabaseInfo
    {
        // --- 하드 코딩된 상수 모두 제거 ---

        // Connection.ini 파일 경로 및 캐싱을 위한 정적 필드
        private static readonly string _configPath;
        private static readonly object _dbLock = new object();
        private static string _cachedDbConnectionString = null;
        private static DateTime _dbCacheLastRead = DateTime.MinValue;

        // ▼▼▼ "공용 키" (EncryptTool/Program.cs의 키와 100% 동일) ▼▼▼
        private const string AES_COMMON_KEY = "greatit-lab-itm-agent-v1-secret";
        // ▲▲▲ 완료 ▲▲▲

        /// <summary>
        /// Connection.ini 파일 경로 정적 생성자
        /// </summary>
        static DatabaseInfo()
        {
            _configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Connection.ini");
        }

        private DatabaseInfo() { }
        public static DatabaseInfo CreateDefault() => new DatabaseInfo();

        /// <summary>
        /// PostgreSQL 전용 연결 문자열 생성 (Connection.ini에서 읽음)
        /// </summary>
        public string GetConnectionString()
        {
            lock (_dbLock)
            {
                // 10초간 캐시된 값 사용 (잦은 파일 접근 방지)
                if (_cachedDbConnectionString != null && (DateTime.Now - _dbCacheLastRead).TotalSeconds < 10)
                {
                    return _cachedDbConnectionString;
                }

                // Connection.ini 파싱 로직
                try
                {
                    string content = ReadAllTextSafe(_configPath);
                    if (string.IsNullOrEmpty(content))
                    {
                        throw new InvalidOperationException("Connection.ini is empty or not found.");
                    }

                    // 1. [Database] Config = ... 값 읽기
                    string encryptedDbConfig = ParseIniValue(content, "Database", "Config");

                    if (string.IsNullOrEmpty(encryptedDbConfig))
                        throw new InvalidOperationException("[Database] Config not found in Connection.ini.");

                    // 2. AES 공용 키로 복호화 (평문 연결 문자열)
                    string plainConnectionString = DecryptAES(encryptedDbConfig, AES_COMMON_KEY);
                    
                    _cachedDbConnectionString = plainConnectionString;
                    _dbCacheLastRead = DateTime.Now;

                    return _cachedDbConnectionString;
                }
                catch (Exception ex)
                {
                    // 파싱 실패 시, 이전에 캐시된 값이라도 반환
                    if (_cachedDbConnectionString != null) return _cachedDbConnectionString;
                    // 캐시조차 없으면 예외 발생
                    throw new InvalidOperationException("Failed to read/parse/decrypt Connection.ini: " + ex.Message, ex);
                }
            }
        }

        // --- 공용 헬퍼 메서드 (Agent가 호출할 수 있도록 public static) ---

        // ▼▼▼ [추가] CS0117 오류의 원인! 누락되었던 래퍼 메서드 ▼▼▼
        /// <summary>
        /// Connection.ini의 특정 섹션에서 키에 해당하는 값을 읽습니다.
        /// </summary>
        public static string GetIniValue(string section, string key)
        {
            try
            {
                string content = ReadAllTextSafe(_configPath);
                return ParseIniValue(content, section, key);
            }
            catch { return null; }
        }
        // ▲▲▲ [추가] 완료 ▲▲▲

        /// <summary>
        /// INI 파일 내용을 파싱합니다. (공용)
        /// </summary>
        public static string ParseIniValue(string fileContent, string section, string key)
        {
            if (string.IsNullOrEmpty(fileContent)) return null;
            
            // (.*)로 빈 값도 허용
            var match = new Regex(
                @"\[" + Regex.Escape(section) + @"\](?:[^\[]*)?" + 
                Regex.Escape(key) + @"\s*=\s*(.*)", 
                RegexOptions.IgnoreCase | RegexOptions.Singleline
            ).Match(fileContent);

            if (match.Success)
            {
                // 값의 첫 줄만 반환 (개행문자가 있을 경우)
                return match.Groups[1].Value.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)[0].Trim();
            }
            return null;
        }

        /// <summary>
        /// 파일 잠김 문제를 회피하며 텍스트를 읽습니다. (FileShare.ReadWrite)
        /// </summary>
        public static string ReadAllTextSafe(string path, int timeoutMs = 5000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (var sr = new StreamReader(fs, Encoding.UTF8))
                    {
                        return sr.ReadToEnd();
                    }
                }
                catch (FileNotFoundException) { return null; } // 파일이 없으면 즉시 null 반환
                catch (IOException) 
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                        throw new TimeoutException($"Failed to read {path} within {timeoutMs}ms.");
                    Thread.Sleep(300);
                }
            }
        }
        
        /// <summary>
        /// 파일 잠김 문제를 회피하며 텍스트를 씁니다. (Agent가 사용)
        /// </summary>
        public static void WriteAllTextSafe(string path, string content, int timeoutMs = 5000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                    using (var writer = new StreamWriter(fs, Encoding.UTF8))
                    {
                        writer.Write(content);
                        return; // 성공
                    }
                }
                catch (IOException)
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                        throw new TimeoutException($"Failed to write {path} within {timeoutMs}ms.");
                    Thread.Sleep(300);
                }
            }
        }

        // --- AES 헬퍼 (공용 키 사용, 내부 전용) ---

        /// <summary>
        /// (EncryptTool의 EncryptAES와 호환됨)
        /// </summary>
        private static string DecryptAES(string cipherText, string keyString)
        {
            byte[] fullCipher = Convert.FromBase64String(cipherText);
            byte[] keyBytes;
            using (var sha = SHA256.Create())
            {
                keyBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(keyString));
            }

            byte[] iv = new byte[16];
            byte[] cipher = new byte[fullCipher.Length - 16];
            Buffer.BlockCopy(fullCipher, 0, iv, 0, 16);
            Buffer.BlockCopy(fullCipher, 16, cipher, 0, fullCipher.Length - 16);

            using (var aes = new AesManaged())
            {
                aes.Key = keyBytes;
                aes.IV = iv;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream(cipher))
                using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (var sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        // --- DPAPI 헬퍼 메서드 (모두 삭제됨) ---

        /// <summary>
        /// DB 연결 테스트(콘솔 전용)
        /// </summary>
        public void TestConnection()
        {
            Console.WriteLine($"[DB] Connection Check Start...");
            string cs = GetConnectionString(); // 이 시점에서 INI 파일 읽기 및 복호화 시도
            Console.WriteLine($"[DB] ConnectionString (Masked) ▶ {Regex.Replace(cs, "Password=.*", "Password=***")}");

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                Console.WriteLine($"[DB] 연결 성공 ▶ {conn.PostgreSqlVersion}");
            }
        }
    }

    /// <summary>
    /// FileZilla Server (FTPS) 접속 정보를 관리합니다. (Connection.ini에서 읽음)
    /// </summary>
    public sealed class FtpsInfo
    {
        // ▼▼▼ [수정] Config = ... 읽도록 변경 ▼▼▼
        private static readonly object _ftpLock = new object();
        private static FtpConfig _cachedFtpConfig = null;
        private static DateTime _ftpCacheLastRead = DateTime.MinValue;

        // ▼▼▼ "공용 키" (DatabaseInfo의 키와 100% 동일) ▼▼▼
        private const string AES_COMMON_KEY = "greatit-lab-itm-agent-v1-secret";
        
        private FtpConfig GetFtpConfig()
        {
             lock (_ftpLock)
            {
                // 10초간 캐시된 값 사용
                if (_cachedFtpConfig != null && (DateTime.Now - _ftpCacheLastRead).TotalSeconds < 10)
                {
                    return _cachedFtpConfig;
                }
                
                try
                {
                    string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Connection.ini");
                    string content = DatabaseInfo.ReadAllTextSafe(configPath);
                    // ▼▼▼ [수정] DatabaseInfo.ParseIniValue 헬퍼 사용 ▼▼▼
                    string encryptedFtpConfig = DatabaseInfo.ParseIniValue(content, "Ftps", "Config");
                    
                    if (string.IsNullOrEmpty(encryptedFtpConfig))
                        throw new InvalidOperationException("[Ftps] Config not found in Connection.ini.");
                        
                    // AES 공용 키로 복호화 (평문 JSON)
                    string plainJson = DecryptAES_Internal(encryptedFtpConfig, AES_COMMON_KEY);
                    
                    // JSON 역직렬화
                    _cachedFtpConfig = JsonConvert.DeserializeObject<FtpConfig>(plainJson);
                    _ftpCacheLastRead = DateTime.Now;
                    
                    return _cachedFtpConfig;
                }
                catch (Exception ex)
                {
                    if (_cachedFtpConfig != null) return _cachedFtpConfig;
                    throw new InvalidOperationException("Failed to read/parse/decrypt [Ftps] Config: " + ex.Message, ex);
                }
            }
        }
        
        // ▲▲▲ [수정] 완료 ▲▲▲
        
        // ▼▼▼ [수정] 속성(Property)이 GetFtpConfig()를 호출하도록 변경 ▼▼▼
        public string Host => GetFtpConfig()?.Host;
        public int Port => GetFtpConfig()?.Port ?? 21;
        public string Username => GetFtpConfig()?.Username;
        public string Password => GetFtpConfig()?.Password;
        public string UploadPath => "/"; // (요청사항: 하드 코딩)
        // ▲▲▲ [수정] 완료 ▲▲▲

        private FtpsInfo() { }
        public static FtpsInfo CreateDefault() => new FtpsInfo();

        // ▼▼▼ [추가] FtpsInfo 전용 DecryptAES 헬퍼 (DatabaseInfo의 것과 100% 동일) ▼▼▼
        private static string DecryptAES_Internal(string cipherText, string keyString)
        {
            byte[] fullCipher = Convert.FromBase64String(cipherText);
            byte[] keyBytes;
            using (var sha = SHA256.Create())
            {
                keyBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(keyString));
            }
            byte[] iv = new byte[16];
            byte[] cipher = new byte[fullCipher.Length - 16];
            Buffer.BlockCopy(fullCipher, 0, iv, 0, 16);
            Buffer.BlockCopy(fullCipher, 16, cipher, 0, fullCipher.Length - 16);
            using (var aes = new AesManaged())
            {
                aes.Key = keyBytes;
                aes.IV = iv;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                using (var ms = new MemoryStream(cipher))
                using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                using (var sr = new StreamReader(cs))
                {
                    return sr.ReadToEnd();
                }
            }
        }
        // ▲▲▲ [추가] 완료 ▲▲▲
    }
}
