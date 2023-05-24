using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using PHRLockerAPI.DBContext;
using PHRLockerAPI.Models;
using PHRLockerAPI.Models.MtmBenfModel;
using PHRLockerAPI.ViewModel;
using System;
using System.Data;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace PHRLockerAPI.Controllers
{
    //[Route("api/[controller]")]
    [ApiController]
    public class MTMBlockController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly DapperContext _context;
        public MTMBlockController(IConfiguration configuration, DapperContext context)
        {
            _configuration = configuration;
            _context = context;
        }

        string fdate = "2000-01-01";
        string tdate = "2040-12-31";


        [HttpGet]
        [Route("gethtblock")]
        public async Task<List<gethtblockModel>> getht()
        {
            string query = "SELECT * FROM public.gethtblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<gethtblockModel>(query);
                return results.ToList();
            }
        }


        [HttpGet]
        [Route("getdtblock")]
        public async Task<List<getdtblockModel>> getdt()
        {
            string query = "SELECT * FROM public.getdtblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getdtblockModel>(query);
                return results.ToList();
            }
        }


        [HttpGet]
        [Route("gethtdtblock")]
        public async Task<List<gethtdtBlockModel>> gethtdt()
        {
            string query = "SELECT * FROM public.gethtdtblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<gethtdtBlockModel>(query);
                return results.ToList();
            }
        }
        [HttpGet]
        [Route("getpallativeblock")]
        public async Task<List<getpallativeblock>> getpallative()
        {
            string query = "SELECT * FROM public.getpallativeblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getpallativeblock>(query);
                return results.ToList();
            }
        }

        [HttpGet]
        [Route("getphysioblock")]
        public async Task<List<getphysioblock>> getphysio()
        {
            string query = "SELECT * FROM public.getphysioblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getphysioblock>(query);
                return results.ToList();
            }
        }

        [HttpGet]
        [Route("getcapdblock")]
        public async Task<List<getcapdblock>> getcapd()
        {
            string query = "SELECT * FROM public.getcapdblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getcapdblock>(query);
                return results.ToList();
            }
        }

        [HttpGet]
        [Route("mtmkpiscreeningblock")]
        public async Task<List<mtmkpiscreeningblock>> mtmkpiscreening()
        {
            string query = "SELECT * FROM public.mtmkpiscreeningblock()";

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<mtmkpiscreeningblock>(query);
                return results.ToList();
            }
        }

    }
}
