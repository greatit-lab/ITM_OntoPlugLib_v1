// ITM_OntoPlugLib/ConnectInfo/DatabaseInfo.cs
using System;
using Npgsql;

namespace ConnectInfo
{
    public sealed class DatabaseInfo
    {
        /* ── 하드코딩 예시는 그대로 두고 포트만 PostgreSQL 기본값(5432) ── */
        private const string _server = "00.000.00.00";
        private const string _database = "itm";
        private const string _userId = "userid";
        private const string _password = "pw";
        private const int _port = 5432;

        // 외부에서 서버 주소를 참조할 수 있도록 public 속성 추가
        public string ServerAddress => _server;

        private DatabaseInfo() { }
        public static DatabaseInfo CreateDefault() => new DatabaseInfo();

        /// <summary>
        /// PostgreSQL 전용 연결 문자열 생성
        /// </summary>
        public string GetConnectionString()
        {
            var csb = new NpgsqlConnectionStringBuilder
            {
                Host = _server,
                Database = _database,
                Username = _userId,
                Password = _password,
                Port = _port,
                Encoding = "UTF8",
                SslMode = SslMode.Disable,   // 필요 시 Enable 로 변경
                // ▼ 기본 스키마를 public 으로 지정
                SearchPath = "public"
            };
            return csb.ConnectionString;
        }

        /// <summary>
        /// DB 연결 테스트(콘솔 전용)
        /// </summary>
        public void TestConnection()
        {
            Console.WriteLine($"[DB] Connection ▶ {GetConnectionString()}");

            using (var conn = new NpgsqlConnection(GetConnectionString()))
            {
                conn.Open();
                Console.WriteLine($"[DB] 연결 성공 ▶ {conn.PostgreSqlVersion}");
            }
        }
    }

    /// <summary>
    /// FileZilla Server (FTPS) 접속 정보를 관리합니다.
    /// </summary>
    public sealed class FtpsInfo
    {
        // ▼▼▼ DB와 FTP 서버가 동일하므로, DatabaseInfo의 서버 주소를 그대로 사용합니다. ▼▼▼
        private readonly string _host = DatabaseInfo.CreateDefault().ServerAddress;

        // ▼▼▼ FileZilla Server에서 설정한 포트 번호 (FTPS 기본값: 21) ▼▼▼
        private const int _port = 21;

        // ▼▼▼ FileZilla Server에서 생성한 사용자 계정 정보 ▼▼▼
        private const string _username = "itm_agent_user";
        private const string _password = "your_filezilla_password";

        // PDF 파일이 저장될 서버의 기본 경로
        private const string _uploadPath = "/";

        // 외부에서 사용할 수 있도록 속성(Property)으로 정보를 노출합니다.
        public string Host => _host;
        public int Port => _port;
        public string Username => _username;
        public string Password => _password;
        public string UploadPath => _uploadPath;

        private FtpsInfo() { }
        public static FtpsInfo CreateDefault() => new FtpsInfo();
    }
}
