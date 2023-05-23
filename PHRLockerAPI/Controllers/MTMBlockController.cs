using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
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
        public MTMBlockController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        string fdate = "2000-01-01";
        string tdate = "2040-12-31";


        [HttpGet]
        [Route("gethtblock")]
        public List<gethtblockModel> getht()
        {
            List<gethtblockModel> RList = new List<gethtblockModel>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.gethtblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new gethtblockModel
                {
                    block_id = drInner["block_id"].ToString(),
                    ht = double.Parse(drInner["TotalCount"].ToString()),
                });

            }

            return RList;
        }
        [HttpGet]
        [Route("getdtblock")]
        public List<getdtblockModel> getdt()
        {
            List<getdtblockModel> RList = new List<getdtblockModel>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.getdtblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new getdtblockModel
                {
                    block_id = drInner["block_id"].ToString(),
                    dt = double.Parse(drInner["totalcount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("gethtdtblock")]
        public List<gethtdtBlockModel> gethtdt()
        {
            List<gethtdtBlockModel> RList = new List<gethtdtBlockModel>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.gethtdtblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new gethtdtBlockModel
                {
                    block_id = drInner["block_id"].ToString(),
                    htdt = double.Parse(drInner["totalcount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getpallativeblock")]
        public List<getpallativeblock> getpallative()
        {
            List<getpallativeblock> RList = new List<getpallativeblock>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.getpallativeblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new getpallativeblock
                {
                    block_id = drInner["block_id"].ToString(),
                    pallative = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getphysioblock")]
        public List<getphysioblock> getphysio()
        {
            List<getphysioblock> RList = new List<getphysioblock>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.getphysioblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new getphysioblock
                {
                    block_id = drInner["block_id"].ToString(),
                    physio = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getcapdblock")]
        public List<getcapdblock> getcapd()
        {
            List<getcapdblock> RList = new List<getcapdblock>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();
            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.getcapdblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new getcapdblock
                {
                    block_id = drInner["block_id"].ToString(),
                    capd = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }

        [HttpGet]
        [Route("mtmkpiscreeningblock")]
        public List<mtmkpiscreeningblock> mtmkpiscreening()
        {
            List<mtmkpiscreeningblock> RList = new List<mtmkpiscreeningblock>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select * from public.mtmkpiscreeningblock()";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpiscreeningblock
                {
                    block_id = drInner["block_id"].ToString(),
                    uniquescreening = double.Parse(drInner["uniquescount"].ToString()),
                    totalscreening = double.Parse(drInner["totalscreening"].ToString()),

                });
            }
            con.Close();
            return RList;
        }
    }
}
