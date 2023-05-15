using Npgsql;
using System.Data;

namespace PHRLockerAPI.DBContext
{
    public class DapperContext
    {
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        public DapperContext(IConfiguration configuration)
        {
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("Constring");
        }
        public IDbConnection CreateConnection() => new NpgsqlConnection(_connectionString);
    }
}
