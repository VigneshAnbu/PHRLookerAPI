using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using PHRLockerAPI.DBContext;
using PHRLockerAPI.Models;
using PHRLockerAPI.Models.MtmBenfModel;
using PHRLockerAPI.ViewModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace PHRLockerAPI.Controllers
{
    //[Route("api/[controller]")]
    [ApiController]
    public class WebAPI3Controller : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly DapperContext _context;
        public WebAPI3Controller(IConfiguration configuration, DapperContext context)
        {
            _configuration = configuration;
            _context = context;
        }

        string fdate = "2000-01-01";
        string tdate = "2040-12-31";

        [HttpGet]
        [Route("getdistrictmaster")]
        public VMCommunityTriage getdistrictmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_name,MS.district_gid,MS.district_id from  public.address_district_master as MS ";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {

                var SList = new CommunityTriageModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();
                //SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();

            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [Route("gethudmaster")]
        public VMCommunityTriage gethudmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid,MS.district_id from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<HudModel> RListHud = new List<HudModel>();

            while (drHud.Read())
            {

                var SList = new HudModel();

                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                SList.hud_id = drHud["hud_id"].ToString();
                SList.district_id = drHud["district_id"].ToString();
                SList.district_gid = drHud["district_gid"].ToString();
                SList.district_name = drHud["district_name"].ToString();
                //SList.TotalCount = drHud["TotalCount"].ToString();
                RListHud.Add(SList);
            }
            con.Close();
            VM.HudWise = RListHud;
            return VM;
        }
        [HttpGet]
        [Route("getblockmaster")]
        public getblockmasterModel getblockmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            getblockmasterModel VM = new getblockmasterModel();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select * from public.getblockMaster()";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<BlockModel> RListHud = new List<BlockModel>();

            while (drHud.Read())
            {

                var SList = new BlockModel();

                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                SList.hud_id = drHud["hud_id"].ToString();
                SList.block_id = drHud["block_id"].ToString();
                SList.block_name = drHud["block_Name"].ToString();
                SList.block_gid = drHud["block_gid"].ToString();
                SList.district_name = drHud["district_name"].ToString();
                SList.district_gid = drHud["district_gid"].ToString();
                //SList.TotalCount = drHud["TotalCount"].ToString();
                RListHud.Add(SList);
            }
            con.Close();
            VM.BlockWise = RListHud;
            return VM;
        }

        private string formatchange(string datestr)
        {
            //string newdate = "";
            //try
            //{
            //    string[] str12 = datestr.Split('/');
            //    newdate = str12[2] + "-" + str12[0] + "-" + str12[1];
            //    return newdate;
            //}
            //catch (Exception ex)
            //{ }
            //return "";
            return datestr;
        }
        [HttpGet]
        [Route("getwebdashboard")]
        public VMCommunityTriage getwebdashboard(string? fromdate, string? todate)
        {
            if (fromdate == null || fromdate == "" || todate == "" || todate == null)
            {
                fromdate = fdate;
                todate = tdate;
            }
            else
            {
                fromdate = formatchange(fromdate);
                todate = formatchange(todate);
            }

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "SELECT MS.DISTRICT_NAME,DISTRICT_GID,COUNT(M.FAMILY_ID) TOTALCOUNT FROM PUBLIC.ADDRESS_DISTRICT_MASTER AS MS left JOIN PUBLIC.FAMILY_MEMBER_MASTER AS M ON MS.DISTRICT_ID = M.DISTRICT_ID   and M.last_update_date::date between '" + fromdate + "' and '" + todate + "'  GROUP BY MS.DISTRICT_NAME,MS.DISTRICT_GID";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                var SList = new CommunityTriageModel();
                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();
                SList.age_18_30 = "0";
                SList.above30 = "0";
                SList.screeningCount = "0";
                SList.drugcount = "0";
                SList.mtmcount = "0";
                RList.Add(SList);
            }
            con.Close();
            con.Open();

            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT MS.DISTRICT_GID,CASE WHEN DATE_PART('year', AGE(BIRTH_DATE)) BETWEEN 18 AND 45 THEN '18 to 30' WHEN DATE_PART('year', AGE(BIRTH_DATE)) BETWEEN 46 AND 150 THEN 'Above 30' END AGETEXT, COUNT(M.FAMILY_ID) TOTALCOUNT FROM PUBLIC.ADDRESS_DISTRICT_MASTER AS MS INNER JOIN PUBLIC.FAMILY_MEMBER_MASTER AS M ON MS.DISTRICT_ID = M.DISTRICT_ID where M.last_update_date::date between '" + fromdate + "' and '" + todate + "' GROUP BY MS.DISTRICT_GID,AGETEXT";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                CommunityTriageModel SList = new CommunityTriageModel();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;

                        if (drInner["AgeText"].ToString() == "Below 18" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].below18 = drInner["TOTALCOUNT"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TOTALCOUNT"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].above30 = drInner["TOTALCOUNT"].ToString();
                        }

                    }
                }
            }

            con.Close();
            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID,COUNT(HH.MEMBER_ID) TOTALCOUNT FROM HEALTH_SCREENING HH INNER JOIN FAMILY_MASTER FM ON HH.FAMILY_ID = FM.FAMILY_ID INNER JOIN ADDRESS_DISTRICT_MASTER DM ON FM.DISTRICT_ID = DM.DISTRICT_ID where HH.last_update_date::date between  '" + fromdate + "' and '" + todate + "'  GROUP BY FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //mtm beneficiary
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID,COUNT(HH.MEMBER_ID) TOTALCOUNT FROM HEALTH_HISTORY HH INNER JOIN FAMILY_MASTER FM ON HH.FAMILY_ID = FM.FAMILY_ID INNER JOIN ADDRESS_DISTRICT_MASTER DM ON FM.DISTRICT_ID = DM.DISTRICT_ID WHERE CAST (MTM_BENEFICIARY -> 'avail_service' AS text) = '\"yes\"' and HH.last_update_date::date between '" + fromdate + "' and '" + todate + "'  GROUP BY FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].mtmcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //drug delivered
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT MS.DISTRICT_GID,COUNT(S.MEMBER_ID) TOTALCOUNT FROM PUBLIC.HEALTH_SCREENING AS S INNER JOIN PUBLIC.FAMILY_MASTER AS M ON M.FAMILY_ID = S.FAMILY_ID INNER JOIN PUBLIC.ADDRESS_DISTRICT_MASTER AS MS ON M.DISTRICT_ID = MS.DISTRICT_ID WHERE S.DRUGS != 'null' and S.last_update_date::date between '" + fromdate + "' and '" + todate + "'  GROUP BY MS.DISTRICT_GID";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].drugcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            VM.DistrictWise = RList;
            return VM;
        }

        [HttpGet]
        [Route("gethudbydistrict")]
        public VMCommunityTriage gethudbydistrict(string? districtid, string? fromdate, string? todate)
        {
            if (fromdate == null || fromdate == "" || todate == "" || todate == null)
            {
                fromdate = fdate;
                todate = tdate;
            }
            else
            {
                fromdate = formatchange(fromdate);
                todate = formatchange(todate);
            }

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount from   public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id  left join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id and M.last_update_date::date between '" + fromdate + "' and '" + todate + "'   where MS.district_gid in (" + districtid + ") group by MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                var SList = new CommunityTriageModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_id = dr["hud_id"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();
                SList.age_18_30 = "0";
                SList.above30 = "0";
                SList.screeningCount = "0";
                SList.drugcount = "0";
                SList.mtmcount = "0";
                RList.Add(SList);
            }
            con.Close();
            con.Open();

            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount,CASE             WHEN date_part('year',age(birth_date)) between 18 and 45 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 46 and 150 THEN 'Above 30' END AgeText from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id inner join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id   where ms.district_gid in (" + districtid + ") and M.last_update_date::date between '" + fromdate + "' and '" + todate + "'   group by MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid,AgeText";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                CommunityTriageModel SList = new CommunityTriageModel();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        //if (drInner["AgeText"].ToString() == "Below 18" && SList.hud_id == drInner["hud_id"].ToString())
                        //{
                        //    RList[i].below18 = drInner["count2"].ToString();
                        //}
                        if (drInner["AgeText"].ToString() == "18 to 30" && RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" && RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].above30 = drInner["TotalCount"].ToString();
                        }

                    }
                }
            }

            con.Close();
            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select fm.district_id,district_name,district_gid,hm.hud_id,hm.hud_gid,hm.hud_name,count(hh.member_id) TotalCount from health_screening hh inner join family_master fm on hh.family_id = fm.family_id inner join address_district_master dm on fm.district_id = dm.district_id inner join address_hud_master hm on hm.hud_id = fm.hud_id where district_gid in (" + districtid + ") and hh.last_update_date::date between '" + fromdate + "' and '" + todate + "' group by fm.district_id,district_name,district_gid,hm.hud_id,hm.hud_gid,hm.hud_name";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //mtm beneficiary
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select hm.hud_id,hm.hud_gid,hm.hud_name,count(hh.member_id) TotalCount from health_history hh inner join family_master fm on hh.family_id=fm.family_id  inner join address_district_master dm on dm.district_id=fm.district_id inner join address_hud_master hm on hm.hud_id=fm.hud_id where dm.district_gid in (" + districtid + ") and CAST (mtm_beneficiary -> 'avail_service' as text)='\"yes\"' and hh.last_update_date::date between '" + fromdate + "' and '" + todate + "' group by hm.hud_id,hm.hud_gid,hm.hud_name";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].mtmcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //drug delivered
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select MS.hud_name,MS.hud_id,count(S.member_id) TotalCount from public.health_screening as S inner join public.family_master as M on M.family_id=S.family_id  inner join address_district_master dm on dm.district_id=M.district_id inner join public.address_hud_master as MS on M.hud_id=MS.hud_id where s.drugs!='null' and dm.district_gid in (" + districtid + ") and S.last_update_date::date between '" + fromdate + "' and '" + todate + "' group by MS.hud_name,MS.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].drugcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [Route("getblockbyhud")]
        public VMCommunityTriage getblockbyhud(string? hudid, string? fromdate, string? todate)
        {
            if (fromdate == null || fromdate == "" || todate == "" || todate == null)
            {
                fromdate = fdate;
                todate = tdate;
            }
            else
            {
                fromdate = formatchange(fromdate);
                todate = formatchange(todate);
            }
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(fm.family_id) TotalCount,block_name,block_gid from public.address_block_master as bm inner join public.address_district_master as MS on bm.district_id=MS.district_id inner join public.address_hud_master as hu on bm.hud_id=hu.hud_id left join public.family_member_master as fm on fm.block_id=bm.block_id and hu.hud_id=fm.hud_id and fm.last_update_date::date between '" + fromdate + "' and '" + todate + "' where hu.hud_gid=" + hudid + " group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,block_name,block_gid";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                var SList = new CommunityTriageModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.block_name = dr["block_name"].ToString();
                SList.block_gid = dr["block_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();
                SList.age_18_30 = "0";
                SList.above30 = "0";
                SList.screeningCount = "0";
                SList.drugcount = "0";
                SList.mtmcount = "0";

                RList.Add(SList);
            }
            con.Close();
            con.Open();

            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT bm.block_gid,COUNT(M.FAMILY_ID) TOTALCOUNT,CASE WHEN DATE_PART('year', AGE(BIRTH_DATE)) BETWEEN 18 AND 45 THEN '18 to 30' WHEN DATE_PART('year',AGE(BIRTH_DATE)) BETWEEN 46 AND 150 THEN 'Above 30' END AGETEXT FROM public.ADDRESS_DISTRICT_MASTER AS MS inner JOIN PUBLIC.ADDRESS_HUD_MASTER AS HU ON MS.DISTRICT_ID = HU.DISTRICT_ID  inner JOIN PUBLIC.FAMILY_MEMBER_MASTER AS M ON MS.DISTRICT_ID = M.DISTRICT_ID AND hu.HUD_ID = M.HUD_ID inner join address_block_master bm on bm.block_id= M.block_id where hud_gid = " + hudid + " and M.last_update_date::date between '" + fromdate + "' and '" + todate + "' GROUP BY MS.DISTRICT_NAME,bm.block_gid,AGETEXT";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                CommunityTriageModel SList = new CommunityTriageModel();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        //if (drInner["AgeText"].ToString() == "Below 18" && SList.hud_id == drInner["hud_id"].ToString())
                        //{
                        //    RList[i].below18 = drInner["count2"].ToString();
                        //}
                        if (drInner["AgeText"].ToString() == "18 to 30" && RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" && RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].above30 = drInner["TotalCount"].ToString();
                        }

                    }
                }
            }

            con.Close();
            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID,HM.HUD_ID,HM.HUD_GID,HM.HUD_NAME,bm.block_gid,block_name,COUNT(HH.MEMBER_ID) TOTALCOUNT FROM HEALTH_SCREENING HH INNER JOIN FAMILY_MASTER FM ON HH.FAMILY_ID = FM.FAMILY_ID INNER JOIN ADDRESS_DISTRICT_MASTER DM ON FM.DISTRICT_ID = DM.DISTRICT_ID INNER JOIN ADDRESS_HUD_MASTER HM ON HM.HUD_ID = FM.HUD_ID inner join Address_block_master bm on bm.block_id = fm.block_id where HM.hud_gid=" + hudid + " and HH.last_update_date::date between '" + fromdate + "' and '" + todate + "' GROUP BY FM.DISTRICT_ID,DISTRICT_NAME,DISTRICT_GID,HM.HUD_ID,HM.HUD_GID,HM.HUD_NAME,bm.block_gid,block_name";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //mtm beneficiary
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT HM.HUD_ID,HM.HUD_GID,HM.HUD_NAME,bm.block_gid,COUNT(HH.MEMBER_ID) TOTALCOUNT FROM HEALTH_HISTORY HH INNER JOIN FAMILY_MASTER FM ON HH.FAMILY_ID = FM.FAMILY_ID INNER JOIN ADDRESS_DISTRICT_MASTER DM ON DM.DISTRICT_ID = FM.DISTRICT_ID INNER JOIN ADDRESS_HUD_MASTER HM ON HM.HUD_ID = FM.HUD_ID INNER JOIN ADDRESS_BLOCK_MASTER BM ON BM.BLOCK_ID = FM.BLOCK_ID WHERE HM.hud_gid=" + hudid + " and HH.last_update_date::date between '" + fromdate + "' and '" + todate + "' and CAST(MTM_BENEFICIARY -> 'avail_service' AS text) = '\"yes\"' GROUP BY HM.HUD_ID,HM.HUD_GID,HM.HUD_NAME,bm.block_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].mtmcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                //drug delivered
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT MS.HUD_NAME,MS.HUD_ID,bm.block_gid,COUNT(S.MEMBER_ID) TOTALCOUNT FROM PUBLIC.HEALTH_SCREENING AS S INNER JOIN PUBLIC.FAMILY_MASTER AS M ON M.FAMILY_ID = S.FAMILY_ID INNER JOIN ADDRESS_DISTRICT_MASTER DM ON DM.DISTRICT_ID = M.DISTRICT_ID INNER JOIN PUBLIC.ADDRESS_HUD_MASTER AS MS ON M.HUD_ID = MS.HUD_ID inner join address_block_master bm on bm.block_id = M.block_id WHERE S.DRUGS != 'null' and MS.hud_gid=" + hudid + " and S.last_update_date::date between '" + fromdate + "' and '" + todate + "' GROUP BY MS.HUD_NAME,MS.HUD_ID,bm.block_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].drugcount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [Route("getuserperstate")]
        public VMUserPerformance GetUserPerformance()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMUserPerformance VM = new VMUserPerformance();

            int IN24 = 0;
            int IN30 = 0;
            int FS24 = 0;
            int FS30 = 0;
            int DI24 = 0;
            int DI30 = 0;


            int T_SyncedScreenings24 = 0;
            int T_SyncedScreenings48 = 0;
            int T_SyncedScreenings30 = 0;


            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid from public.address_district_master as MS order by district_name";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<UserPerformanceModel> RList = new List<UserPerformanceModel>();

            while (dr.Read())
            {

                var SList = new UserPerformanceModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();

                RList.Add(SList);
            }


            con.Close();



            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-24 hours' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening24 = drInner["count"].ToString();

                            T_SyncedScreenings24 = Convert.ToInt32(T_SyncedScreenings24 + RList[i].syncedscreening24);

                        }

                    }
                }

            }


            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-48 hours' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening48 = drInner["count"].ToString();

                            T_SyncedScreenings48 = Convert.ToInt32(T_SyncedScreenings48 + RList[i].syncedscreening48);

                        }

                    }
                }

            }


            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-30 day' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening30 = drInner["count"].ToString();

                            T_SyncedScreenings30 = Convert.ToInt32(T_SyncedScreenings30 + RList[i].syncedscreening30);

                        }

                    }
                }

            }


            con.Close();




            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-24 hours' < S.last_update_date group by S.member_id,M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].individualscreenings24 = drInner["count"].ToString();

                            IN24 = IN24 + Convert.ToInt32(RList[i].individualscreenings24);
                        }

                    }
                }

            }


            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where  now()+ interval '-30 day' < S.last_update_date group by S.member_id,M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].individualscreenings30 = drInner["count"].ToString();
                            IN30 = IN30 + Convert.ToInt32(RList[i].individualscreenings30);
                        }

                    }
                }

            }


            con.Close();

            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-24 hours' < S.last_update_date group by S.family_id,M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].familyscreenings24 = drInner["count"].ToString();

                            FS24 = FS24 + Convert.ToInt32(RList[i].familyscreenings24);
                        }

                    }
                }

            }


            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where now()+ interval '-30 day' < S.last_update_date group by S.family_id,M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].familyscreenings30 = drInner["count"].ToString();

                            FS30 = FS30 + Convert.ToInt32(RList[i].familyscreenings30);
                        }

                    }
                }

            }


            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where  S.drugs!='null' and now()+ interval '-24 hours' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].drugissued24 = drInner["count"].ToString();

                            DI24 = DI24 + Convert.ToInt32(RList[i].drugissued24);
                        }

                    }
                }

            }


            con.Close();



            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id where S.drugs!='null' and now()+ interval '-30 day' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].drugissued30 = drInner["count"].ToString();

                            DI30 = DI30 + Convert.ToInt32(RList[i].drugissued30);
                        }

                    }
                }

            }


            con.Close();


            VM.UserList = RList;


            VM.individualscreeningsaverage = (IN30 / 30).ToString();
            VM.familyscreeningsaverage = (FS30 / 30).ToString();
            VM.drugissuedaverage = (DI30 / 30).ToString();
            VM.T_SyncedScreenings24 = T_SyncedScreenings24.ToString();
            VM.T_SyncedScreenings48 = T_SyncedScreenings48.ToString();
            VM.T_SyncedScreenings30 = T_SyncedScreenings30.ToString();
            VM.T_FamilyScreenings24 = FS24.ToString();
            VM.T_FamilyScreenings30 = FS30.ToString();
            VM.T_DrugIssued24 = DI24.ToString();
            VM.T_DrugIssued30 = DI30.ToString();
            VM.T_IndividualScreenings24 = IN24.ToString();
            VM.T_IndividualScreenings30 = IN30.ToString();

            return VM;
        }    

        [HttpGet]
        [Route("getwebpopulation")]
        public Object getwebpopulation(string? districtid, string? hudid, string? blockid, string? fromdate, string? todate)
        {
            if (fromdate == null || fromdate == "" || todate == "" || todate == null)
            {
                fromdate = fdate;
                todate = tdate;
            }
            else
            {
                fromdate = formatchange(fromdate);
                todate = formatchange(todate);
            }
            double totpopcount = 0;
            double totfsmily = 0;
            double avgfami = 0;
            double sexratio = 0;
            double verified = 0;

            string districtcontition = "";
            string hudcondition = "";
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            if (districtid != null && districtid != "")
                districtcontition = " and district_id='" + districtid + "'";
            if (hudid != null && hudid != "")
                districtcontition = " and hud_id='" + hudid + "'";
            if (blockid != null && blockid != "")
                districtcontition = " and block_id='" + blockid + "'";
            cmdHud.CommandText = "select consent_status,count(member_id) Totalcount from family_member_master where last_update_date::date between '" + fromdate + "' and '" + todate + "' " + districtcontition + " group by consent_status ";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();

            while (drHud.Read())
            {
                totpopcount += int.Parse(drHud["TotalCount"].ToString());
                if (drHud["consent_status"].ToString() == "RECEIVED")
                    verified = double.Parse(drHud["TotalCount"].ToString());
            }
            con.Close();
            con.Open();
            if (totpopcount > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select count(family_id) TotalCount from family_master  where last_update_date::date between '" + fromdate + "' and '" + todate + "' " + districtcontition + "";
                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    totfsmily = int.Parse(drInner["TotalCount"].ToString());
                }
            }
            con.Close();

            double Male = 0, Female = 0;
            con.Open();
            if (totpopcount > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select gender,count(member_id) Totalcount from family_member_master  where last_update_date::date between '" + fromdate + "' and '" + todate + "'  " + districtcontition + " group by gender";
                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    if (drInner["gender"].ToString() == "Male")
                        Male = double.Parse(drInner["TotalCount"].ToString());
                    if (drInner["gender"].ToString() == "Female")
                        Female = double.Parse(drInner["TotalCount"].ToString());
                }
                sexratio = (Male * 1000) / Female;
            }
            con.Close();
            double marriedmale = 0, marriedfemale = 0;
            con.Open();
            if (totpopcount > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select relationship,count(member_id) TotalCount  from family_member_master where ((relationship='Husband' and gender='Male') or (relationship='Wife' and gender='Female')) and last_update_date::date between '" + fromdate + "' and '" + todate + "'  " + districtcontition + "  group by relationship";
                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    if (drInner["relationship"].ToString() == "Husband")
                        marriedmale = double.Parse(drInner["TotalCount"].ToString());
                    if (drInner["relationship"].ToString() == "Wife")
                        marriedfemale = double.Parse(drInner["TotalCount"].ToString());
                }
                sexratio = (Male * 1000) / Female;
            }
            con.Close();

            avgfami = totpopcount / totfsmily;
            return new { totpopcount = totpopcount, totfamily = totfsmily, averagemem = avgfami.ToString("N2"), sexratio = sexratio.ToString("N2"), verified = verified, male = Male, female = Female, marriedmale = marriedmale, marriedfemale = marriedfemale };
        }

        [HttpGet]
        [Route("getwebagewise")]
        public Object getwebagewise(string? districtid, string? hudid, string? blockid, string? fromdate, string? todate)
        {
            if (fromdate == null || fromdate == "" || todate == "" || todate == null)
            {
                fromdate = fdate;
                todate = tdate;
            }
            else
            {
                fromdate = formatchange(fromdate);
                todate = formatchange(todate);
            }
            string districtcontition = "";
            if (districtid != null && districtid != "")
                districtcontition = " and district_id='" + districtid + "'";
            if (hudid != null && hudid != "")
                districtcontition = " and hud_id='" + hudid + "'";
            if (blockid != null && blockid != "")
                districtcontition = " and block_id='" + blockid + "'";
            int c1_0_1 = 0;
            int c1_2_5 = 0;
            int c1_5_12 = 0;


            int below3 = 0;
            int c2_3to6 = 0;
            int c2_7to9 = 0;
            int c2_10to19 = 0;

            int mtm_18_45 = 0;
            int mtm_46_59 = 0;
            int mtm_60 = 0;
            double age_1 = 0, age_2 = 0, age_3 = 0, age_4 = 0, age_5 = 0, age_6 = 0, age_7 = 0, age_8 = 0, age_9 = 0, age_10 = 0, age_above = 0;

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select DATE_PART('year',AGE(BIRTH_DATE)) age,count(member_id) Totalcount from public.family_member_master where last_update_date::date between '" + fromdate + "' and '" + todate + "' " + districtcontition + " group by age";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();

            while (drHud.Read())
            {
                int age = int.Parse(drHud["age"].ToString());
                if (age >= 0 && age <= 1)
                    c1_0_1 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 2 && age <= 5)
                    c1_2_5 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 6 && age <= 12)
                    c1_5_12 += int.Parse(drHud["TotalCount"].ToString());

                if (age >= 0 && age <= 2)
                    below3 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 3 && age <= 6)
                    c2_3to6 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 7 && age <= 9)
                    c2_7to9 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 10 && age <= 19)
                    c2_10to19 += int.Parse(drHud["TotalCount"].ToString());

                if (age >= 18 && age <= 45)
                    mtm_18_45 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 46 && age <= 59)
                    mtm_46_59 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 60)
                    mtm_60 += int.Parse(drHud["TotalCount"].ToString());


                if (age >= 0 && age <= 10)
                    age_1 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 11 && age <= 20)
                    age_2 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 21 && age <= 30)
                    age_3 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 31 && age <= 40)
                    age_4 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 41 && age <= 50)
                    age_5 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 51 && age <= 60)
                    age_6 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 61 && age <= 70)
                    age_7 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 71 && age <= 80)
                    age_8 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 81 && age <= 90)
                    age_9 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 91 && age <= 100)
                    age_10 += int.Parse(drHud["TotalCount"].ToString());
                if (age >= 101)
                    age_above += int.Parse(drHud["TotalCount"].ToString());

            }
            con.Close();
            List<RoleReport> childlist = new List<RoleReport>();
            childlist.Add(new RoleReport { RoleName = "0 to 1", RoleCount = c1_0_1.ToString(), CountPer = "rgb(26, 115, 232)" });
            childlist.Add(new RoleReport { RoleName = "2 to 5", RoleCount = c1_2_5.ToString(), CountPer = "rgb(215, 42, 152)" });
            childlist.Add(new RoleReport { RoleName = "6 to 12", RoleCount = c1_5_12.ToString(), CountPer = "rgb(26, 115, 232)" });
            List<RoleReport> studentlist = new List<RoleReport>();
            studentlist.Add(new RoleReport { RoleName = "below 3", RoleCount = below3.ToString(), CountPer = "rgb(26, 115, 232)" });
            studentlist.Add(new RoleReport { RoleName = "3 to 6", RoleCount = c2_3to6.ToString(), CountPer = "rgb(215, 42, 152)" });
            studentlist.Add(new RoleReport { RoleName = "7 to 9", RoleCount = c2_7to9.ToString(), CountPer = "rgb(26, 115, 232)" });
            studentlist.Add(new RoleReport { RoleName = "10 to 19", RoleCount = c2_10to19.ToString(), CountPer = "rgb(229, 37, 146)" });

            List<RoleReport> mtmlist = new List<RoleReport>();
            mtmlist.Add(new RoleReport { RoleName = "18 to 45", RoleCount = mtm_18_45.ToString(), CountPer = "rgb(26, 115, 232)" });
            mtmlist.Add(new RoleReport { RoleName = "45 to 59", RoleCount = mtm_46_59.ToString(), CountPer = "rgb(215, 42, 152)" });
            mtmlist.Add(new RoleReport { RoleName = "60 above", RoleCount = mtm_60.ToString(), CountPer = "rgb(26, 115, 232)" });

            List<RoleReport> age_list = new List<RoleReport>();
            age_list.Add(new RoleReport { RoleName = "0 to 10", RoleCount = age_1.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "11 to 20", RoleCount = age_2.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "21 to 30", RoleCount = age_3.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "31 to 40", RoleCount = age_4.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "41 to 50", RoleCount = age_5.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "51 to 60", RoleCount = age_6.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "61 to 70", RoleCount = age_7.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "71 to 80", RoleCount = age_8.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "81 to 90", RoleCount = age_9.ToString(), CountPer = "rgb(124, 179, 66)" });
            age_list.Add(new RoleReport { RoleName = "91 to 100", RoleCount = age_10.ToString(), CountPer = "rgb(124, 179, 662)" });

            age_list.Add(new RoleReport { RoleName = "100 above", RoleCount = age_above.ToString(), CountPer = "rgb(124, 179, 66)" });

            return new { childtarget = childlist, studentlist = studentlist, mtmlist = mtmlist, age_list = age_list };
        }

        [HttpGet]
        [Route("diagnosisreport")]
        public List<DiagnosisReport> diagnosisreport()
        {
            List<DiagnosisReport> dlista = new List<DiagnosisReport>();
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            //VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "SELECT jsonb_array_elements(cast(jsonb_array_elements(b.diseases)->>'disease_list' as jsonb))->>'diagnosis' AS diagnosis,count(screening_id) totalcount FROM public.health_screening b WHERE  jsonb_typeof(b.diseases) = 'array' group by diagnosis order by totalcount desc";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<BlockModel> RListHud = new List<BlockModel>();

            while (drHud.Read())
            {
                var dlist = new DiagnosisReport();
                dlist.diagnosisName = drHud["diagnosis"].ToString();

                dlista.Add(dlist);
            }
            con.Close();
            con.Open();
            if (dlista.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select Drugarray,CASE WHEN age between 0 and 17 THEN 'child' WHEN age >18 THEN 'Adult' END age2,gender,sum(TotalScreened) totc from ( select Drugarray,date_part('year',age(birth_date)) age,gender,sum(TotalScreening) TotalScreened from  (SELECT jsonb_array_elements(cast(jsonb_array_elements(b.diseases)->>'disease_list' as jsonb))->>'diagnosis' AS Drugarray ,member_id,count(screening_id) TotalScreening FROM public.health_screening b WHERE  jsonb_typeof(b.diseases) = 'array' group by Drugarray,member_id) tbl inner join family_member_master fm on tbl.member_id=fm.member_id group by Drugarray,age,gender) tbl group by drugarray,age2,gender";
                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    foreach (var aa in dlista)
                    {
                        if (aa.diagnosisName == drInner["Drugarray"].ToString())
                        {

                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Male")
                            {
                                aa.adultopmale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Female")
                            {
                                aa.adultopfemale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Transgender")
                            {
                                aa.adultoptransgender = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            aa.adulttotal = (aa.adultopmale) + (aa.adultopfemale) + (aa.adultoptransgender);


                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Male")
                            {
                                aa.childopmale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Female")
                            {
                                aa.childopfemale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Transgender")
                            {
                                aa.childoptransgender = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            aa.childtotal = (aa.childopmale) + (aa.childopfemale) + (aa.childoptransgender);

                            aa.totalopmale = aa.adultopmale + aa.childopmale;
                            aa.totalopfemale = aa.adultopfemale + aa.childopfemale;
                            aa.totaloptransgender = aa.adultoptransgender + aa.adultoptransgender;

                            aa.totalop = aa.totalopmale + aa.totalopfemale + aa.totaloptransgender;


                        }
                    }
                    //totfsmily = int.Parse(drInner["TotalCount"].ToString());
                }
            }
            con.Close();
            return dlista;
        }

        [HttpGet]
        [Route("getmtmkidistrict")]
        public async Task<List<getmtmkidistrictModel>> getmtmkidistrict()
        {
           
            string query = "SELECT * FROM  public.getmtmkidistrict(); ";
            using (var connection = _context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<getmtmkidistrictModel>(query);
                return OBJ.ToList();
            }

        }
        [HttpPost]
        [Route("getht")]
        public async Task<List<gethtModel>> getht(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getht()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<gethtModel>(query);

                var htList = RList
                  .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                  .Select(RItem => new gethtModel
                  {
                      district_id = RItem.district_id,
                      ht = results.First(result => result.district_id == RItem.district_id).ht
                  })
                  .ToList();


                return htList;
            }
        }

        [HttpPost]
        [Route("getdt")]
        public async Task<List<getdtModel>> getdt(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getdt()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getdtModel>(query);

                var dtList = RList
                    .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                    .Select(RItem => new getdtModel
                    {
                        district_id = RItem.district_id,
                        dt = results.First(result => result.district_id == RItem.district_id).dt
                    })
                    .ToList();

                return dtList;
            }
        }




        [HttpPost]
        [Route("gethtdt")]
        public async  Task<List<getHtDtModel>> gethtdt(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getHtdt()";
            using (var connection = _context.CreateConnection())
            {
                var results =await connection.QueryAsync<getHtDtModel>(query);

                var htdtList = RList
                    .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                    .Select(RItem => new getHtDtModel
                    {
                        district_id = RItem.district_id,
                        htdt = results.First(result => result.district_id == RItem.district_id).htdt
                    })
                    .ToList();

                return htdtList;
            }
        }



        [HttpPost]
        [Route("getpallative")]
        public async Task<List<getPalliativeModel>> getpallative(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getpallative()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getPalliativeModel>(query);

                var pallativeList = RList
                    .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                    .Select(RItem => new getPalliativeModel
                    {
                        district_id = RItem.district_id,
                        pallative = results.FirstOrDefault(result => result.district_id == RItem.district_id)?.pallative ?? 0
                    })
                    .ToList();

                return pallativeList;
            }
        }

        [HttpPost]
        [Route("getphysio")]
        public async Task<List<getPhysioModel>> getphysio(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getPhysio()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getPhysioModel>(query);

                var physioList = RList
                    .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                    .Select(RItem => new getPhysioModel
                    {
                        district_id = RItem.district_id,
                        physio = results.FirstOrDefault(result => result.district_id == RItem.district_id)?.physio ?? 0
                    })
                    .ToList();

                return physioList;
            }
        }

        [HttpPost]
        [Route("getcapd")]
        public async Task<List<getCapdModel>> getcapd(List<getmtmkidistrictModel> RList)
        {
            string query = "SELECT * FROM public.getCapd()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<getCapdModel>(query);

                var capdList = RList
                    .Where(RItem => results.Any(result => result.district_id == RItem.district_id))
                    .Select(RItem => new getCapdModel
                    {
                        district_id = RItem.district_id,
                        capd = results.FirstOrDefault(result => result.district_id == RItem.district_id)?.capd ?? 0
                    })
                    .ToList();

                return capdList;
            }
        }
        [HttpGet]
        [Route("mtmkpiscreening")]
        public async Task<List<mtmkpiscreening>> mtmkpiscreening()
        {
            string query = "SELECT * FROM public.mtmkpiscreening()";
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<mtmkpiscreening>(query);

                return results.ToList();
            }
        }


    }
}
