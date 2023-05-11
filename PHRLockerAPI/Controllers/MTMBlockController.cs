using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using PHRLockerAPI.Models;
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
        public List<mtmkpi> getht()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'hypertension' is not null  and b.mtm_beneficiary->>'diabetes_mellitus' is null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    ht = double.Parse(drInner["TotalCount"].ToString()),
                });

            }

            return RList;
        }
        [HttpGet]
        [Route("getdtblock")]
        public List<mtmkpi> getdt()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'diabetes_mellitus' is not null and b.mtm_beneficiary->>'hypertension' is null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    dt = double.Parse(drInner["totalcount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("gethtdtblock")]
        public List<mtmkpi> gethtdt()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'diabetes_mellitus' is not null and b.mtm_beneficiary->>'hypertension' is not null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    htdt = double.Parse(drInner["totalcount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getpallativeblock")]
        public List<mtmkpi> getpallative()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'palliative_care' is not null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    pallative = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getphysioblock")]
        public List<mtmkpi> getphysio()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'physiotherapy' is not null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    physio = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }
        [HttpGet]
        [Route("getcapdblock")]
        public List<mtmkpi> getcapd()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();
            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from Health_history b inner join family_master fmm on b.family_id=fmm.family_id where b.mtm_beneficiary->>'avail_service'='yes' and b.mtm_beneficiary->>'dialysis_capd' is not null group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
                {
                    block_id = drInner["block_id"].ToString(),
                    capd = double.Parse(drInner["TotalCount"].ToString()),
                });
            }

            return RList;
        }

        [HttpGet]
        [Route("mtmkpiscreeningblock")]
        public List<mtmkpi> mtmkpiscreening()
        {
            List<mtmkpi> RList = new List<mtmkpi>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            cmdInner.CommandText = "select block_id,count(tbl.member_id) uniquescount,sum(userscreening) totalscreening from  ((select block_id,member_id,count(screening_id) userscreening from health_screening hs inner join family_master fm on  hs.family_id=fm.family_id group by block_id,member_id )) tbl group by block_id";
            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            CommunityTriageModel SList = new CommunityTriageModel();
            while (drInner.Read())
            {
                RList.Add(new mtmkpi
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
