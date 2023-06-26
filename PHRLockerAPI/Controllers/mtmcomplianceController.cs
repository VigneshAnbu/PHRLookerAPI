//using Dapper;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PHRLockerAPI.DBContext;
//using PHRLockerAPI.DBContext;
using PHRLockerAPI.Intfa;
using PHRLockerAPI.Models;
using PHRLockerAPI.Models.MtmBenfModel;
using PHRLockerAPI.ViewModel;
using System.Data;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.Intrinsics.Arm;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using static System.Net.Mime.MediaTypeNames;

namespace PHRLockerAPI.Controllers
{
    public class mtmcomplianceController : Controller
    {

        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        private readonly DapperContext context;

        public mtmcomplianceController(DapperContext context, IConfiguration configuration)
        {
            this.context = context;
            _configuration = configuration;
        }

        string CommunityParam = "";

        string InstitutionParam = "";
        public IActionResult Index()
        {
            return View();
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("mtmcomdash")]
        public uhcmonitoringreport mtmcomdash([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcmonitoringreport VM = new uhcmonitoringreport();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;


            cmd.CommandText = "SELECT * from public.uhcgethypertension('" + CommunityParam + "','" + InstitutionParam + "')";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<districtscreeningcountmodel> RList = new List<districtscreeningcountmodel>();

            while (dr.Read())
            {
                VM.hypertesion = dr["totalcount"].ToString();
            }
            con.Close();
            con.Open();

            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT * from public.uhcgethypertensionsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    VM.htsuspected = drInner["totalcount"].ToString();
                }
            }
            con.Close();
            con.Open();

            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT * from public.uhcgethypertensionsuspectedconfirmed('" + CommunityParam + "','" + InstitutionParam + "')";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    VM.htconfirmed = drInner["totalcount"].ToString();
                }
            }
            con.Close();
            con.Open();

            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT * from public.uhcgethypertensiondrug('" + CommunityParam + "','" + InstitutionParam + "')";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    VM.htdrugissued = drInner["totalcount"].ToString();
                }
            }
            con.Close();

            return VM;
        }



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("mtmcomdistrict")]
        public districtscreeningcountmodel mtmcomdistrict([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            districtscreeningcountmodel VM = new districtscreeningcountmodel();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  group by MS.district_name,MS.district_gid";

            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(fm.family_id) TotalCount from  public.address_district_master as MS  \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id  " + CommunityParam + " \r\ngroup by MS.district_name,MS.district_gid";

            cmd.CommandText = "SELECT * from address_district_master";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<districtscreeningcountmodel> RList = new List<districtscreeningcountmodel>();

            while (dr.Read())
            {
                var SList = new districtscreeningcountmodel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_id = dr["district_id"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = "0";
                SList.screeningCount = "0";
                SList.above30 = "0";
                SList.uniqueCount = "0";
                SList.onescreening = "0";
                SList.screeningCount = "0";
                SList.uniqueCount = "0";
                SList.suspectedconfirmed = "0";
                SList.dmconfirmed = "0";
                SList.tc_count = "0";
                SList.ref_count = "0";
                SList.rc_count = "0";
                RList.Add(SList);
            }
            con.Close();

            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.district_id,cast(sum(TotalCount) as bigint) totalcount from (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER, count(b.member_id) TotalCount, fm.district_id FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id = hh2.member_id group by ARRUSER, fm.district_id) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID INNER JOIN address_block_master abm ON FR.block_id = abm.block_id group by tbl.district_id";
                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.district_id,cast(count(member_id) as bigint) totalcount from (select tbl.district_id,tbl.member_id from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,  b.member_id,fm.district_id  FROM Health_screening b  INNER JOIN family_master fm ON b.family_id = fm.family_id  inner join health_history hh2 on b.member_id=hh2.member_id  group by ARRUSER,fm.district_id,b.member_id) tbl  INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)  INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  INNER JOIN address_block_master abm ON FR.block_id = abm.block_id  group by tbl.district_id,tbl.member_id) tbl group by tbl.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;


                cmdInner.CommandText = "select district_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'hypertension' IS NOT NULL group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].suspectedconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select district_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'diabetes_mellitus' IS NOT NULL group by district_id";
                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].dmconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select district_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and mtm_beneficiary->> 'drugs_from' = 'Government' group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].tc_count = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select district_id,count(screening_id) totalcount from health_screening b \r\ninner join family_master fm on b.family_id=fm.family_id\r\nwhere advices->>'advice_referral_compliance'='1'\r\ngroup by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].rc_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select district_id,count(screening_id) totalcount from health_screening b \r\ninner join health_history hh on b.member_id=hh.member_id\r\ninner join family_master fm on fm.family_id=hh.family_id\r\nwhere (cast(outcome->>'hypertension' as json)->>'referral_place_id'!='' or \r\ncast(outcome->>'diabetes' as json)->>'referral_place_id'!='')\r\nand mtm_beneficiary->>'avail_service'='yes'\r\ngroup by district_id";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].ref_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            VM.screeningCount = "0";
            VM.uniqueCount = "0";
            VM.suspectedconfirmed = "0";
            VM.dmconfirmed = "0";
            VM.tc_count = "0";
            VM.ref_count = "0";
            VM.rc_count = "0";
            for (int i = 0; i < RList.Count; i++)
            {
                RList[i].scrpercentage = Percentage_Cal(int.Parse(RList[i].above30), int.Parse(RList[i].screeningCount));
                VM.screeningCount = (double.Parse(VM.screeningCount) + double.Parse(RList[i].screeningCount)).ToString();
                VM.uniqueCount = (double.Parse(VM.uniqueCount) + double.Parse(RList[i].uniqueCount)).ToString();
                VM.suspectedconfirmed = (double.Parse(VM.suspectedconfirmed) + double.Parse(RList[i].suspectedconfirmed)).ToString();
                VM.dmconfirmed = (double.Parse(VM.dmconfirmed) + double.Parse(RList[i].dmconfirmed)).ToString();
                VM.tc_count = (double.Parse(VM.tc_count) + double.Parse(RList[i].tc_count)).ToString();
                VM.ref_count = (double.Parse(VM.ref_count) + double.Parse(RList[i].ref_count)).ToString();
                VM.rc_count = (double.Parse(VM.rc_count) + double.Parse(RList[i].rc_count)).ToString();
            }
            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("mtmcomhud")]
        public hudscreeningcountmodel mtmcomhud([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            hudscreeningcountmodel VM = new hudscreeningcountmodel();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  group by MS.district_name,MS.district_gid";

            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(fm.family_id) TotalCount from  public.address_district_master as MS  \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id  " + CommunityParam + " \r\ngroup by MS.district_name,MS.district_gid";

            cmd.CommandText = "select adm.district_id,district_name,district_gid,hud_id,hud_name,hud_gid from address_district_master adm  inner join address_hud_master ahm on adm.district_id = ahm.district_id";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<hudscreeningcountmodel> RList = new List<hudscreeningcountmodel>();

            while (dr.Read())
            {
                var SList = new hudscreeningcountmodel();

                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_id = dr["hud_id"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.district_name = dr["district_name"].ToString();
                SList.district_id = dr["district_id"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = "0";
                SList.screeningCount = "0";
                SList.above30 = "0";
                SList.uniqueCount = "0";
                SList.onescreening = "0";
                RList.Add(SList);
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.hud_id,cast(sum(TotalCount) as bigint) totalcount from (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER, count(b.member_id) TotalCount,fm.hud_id FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id = hh2.member_id group by ARRUSER, fm.hud_id) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  group by tbl.hud_id";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n group by fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n inner join address_district_master dm on tbl.district_id=dm.district_id \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
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
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.hud_id,cast(count(member_id) as bigint) totalcount from (select tbl.hud_id,tbl.member_id from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,  b.member_id,fm.hud_id  FROM Health_screening b  INNER JOIN family_master fm ON b.family_id = fm.family_id  inner join health_history hh2 on b.member_id=hh2.member_id  group by ARRUSER,fm.hud_id,b.member_id) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)  INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  INNER JOIN address_block_master abm ON FR.block_id = abm.block_id  group by tbl.hud_id,tbl.member_id) tbl group by tbl.hud_id";

                //cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;


                cmdInner.CommandText = "select hud_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'hypertension' IS NOT NULL group by hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].suspectedconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select hud_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'diabetes_mellitus' IS NOT NULL group by hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].dmconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select hud_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and mtm_beneficiary->> 'drugs_from' = 'Government' group by hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].tc_count = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select hud_id,count(screening_id) totalcount from health_screening b \r\ninner join family_master fm on b.family_id=fm.family_id\r\nwhere advices->>'advice_referral_compliance'='1'\r\ngroup by hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].rc_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select hud_id,count(screening_id) totalcount from health_screening b \r\ninner join health_history hh on b.member_id=hh.member_id\r\ninner join family_master fm on fm.family_id=hh.family_id\r\nwhere (cast(outcome->>'hypertension' as json)->>'referral_place_id'!='' or \r\ncast(outcome->>'diabetes' as json)->>'referral_place_id'!='')\r\nand mtm_beneficiary->>'avail_service'='yes'\r\ngroup by hud_id";


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].ref_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            for (int i = 0; i < RList.Count; i++)
            {
                RList[i].scrpercentage = Percentage_Cal(int.Parse(RList[i].above30), int.Parse(RList[i].screeningCount));
                //RList[i].multiscreening = (int.Parse(RList[i].uniqueCount) - int.Parse(RList[i].onescreening)).ToString();
            }
            VM.hudWise = RList;

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("mtmcomblock")]
        public blockscreeningcountmodel mtmcomblock([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            blockscreeningcountmodel VM = new blockscreeningcountmodel();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select adm.district_id,district_name,district_gid,ahm.hud_id,hud_name,hud_gid,abm.block_id,abm.block_name,abm.block_gid from address_district_master adm inner join address_hud_master ahm on adm.district_id=ahm.district_id inner join address_block_master abm on abm.hud_id=ahm.hud_id";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<blockscreeningcountmodel> RList = new List<blockscreeningcountmodel>();

            while (dr.Read())
            {
                var SList = new blockscreeningcountmodel();

                SList.block_name = dr["block_name"].ToString();
                SList.block_id = dr["block_id"].ToString();
                SList.block_gid = dr["block_gid"].ToString();

                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_id = dr["hud_id"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.district_name = dr["district_name"].ToString();
                SList.district_id = dr["district_id"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = "0";
                SList.screeningCount = "0";
                SList.above30 = "0";
                SList.uniqueCount = "0";
                SList.onescreening = "0";

                RList.Add(SList);
            }
            con.Close();

            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.block_id,cast(sum(TotalCount) as bigint) totalcount from (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER, count(b.member_id) TotalCount,fm.block_id FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id = hh2.member_id group by ARRUSER, fm.block_id) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  group by tbl.block_id";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n group by fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n inner join address_district_master dm on tbl.district_id=dm.district_id \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.block_id,cast(count(member_id) as bigint) totalcount from (select tbl.block_id,tbl.member_id from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,  b.member_id,fm.block_id  FROM Health_screening b  INNER JOIN family_master fm ON b.family_id = fm.family_id  inner join health_history hh2 on b.member_id=hh2.member_id  group by ARRUSER,fm.block_id,b.member_id) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)  INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID group by tbl.block_id,tbl.member_id) tbl group by tbl.block_id";

                //cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;


                cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'hypertension' IS NOT NULL group by block_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].suspectedconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and b.mtm_beneficiary->>'diabetes_mellitus' IS NOT NULL group by block_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].dmconfirmed = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select block_id,count(b.member_id) TotalCount from health_history b inner join family_master fm on b.family_id = fm.family_id where mtm_beneficiary->> 'avail_service' = 'yes' and mtm_beneficiary->> 'drugs_from' = 'Government' group by block_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].tc_count = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select block_id,count(screening_id) totalcount from health_screening b \r\ninner join family_master fm on b.family_id=fm.family_id\r\nwhere advices->>'advice_referral_compliance'='1'\r\ngroup by block_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].rc_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select block_id,count(screening_id) totalcount from health_screening b  inner join health_history hh on b.member_id=hh.member_id inner join family_master fm on fm.family_id=hh.family_id where (cast(outcome->>'hypertension' as json)->>'referral_place_id'!='' or  cast(outcome->>'diabetes' as json)->>'referral_place_id'!='') and mtm_beneficiary->>'avail_service'='yes' group by block_id";


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].ref_count = drInner["totalcount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            for (int i = 0; i < RList.Count; i++)
            {
                //RList[i].scrpercentage = Percentage_Cal(int.Parse(RList[i].above30), int.Parse(RList[i].screeningCount));
                //RList[i].multiscreening = (int.Parse(RList[i].uniqueCount) - int.Parse(RList[i].onescreening)).ToString();
            }
            VM.hudWise = RList;

            return VM;
        }



        [HttpGet]
        [Route("FilterAll")]
        public void Filterforall(FilterpayloadModel F)
        {
            if (F.district_id != null && F.district_id != "")
            {
                string Disparam = "";

                if (F.district_id.Contains(","))
                {
                    string[] DistrictValue = F.district_id.Split(",");

                    int i = 0;

                    foreach (var v in DistrictValue)
                    {
                        if (i == (DistrictValue.Length - 1))
                        {
                            Disparam = Disparam + "(fm.district_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fm.district_id = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fm.district_id = '" + F.district_id + "')";
                }


                CommunityParam = Disparam;

            }
            if (F.hud_id != "" && F.hud_id != null)
            {


                string Disparam = "";

                if (F.hud_id.Contains(","))
                {
                    int i = 0;

                    string[] HudValue = F.hud_id.Split(",");

                    foreach (var v in HudValue)
                    {
                        if (i == (HudValue.Length - 1))
                        {
                            Disparam = Disparam + "(fm.hud_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fm.hud_id = '" + v + "') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fm.hud_id = '" + F.hud_id + "')";
                }


                CommunityParam = CommunityParam + Disparam;
            }
            if (F.block_id != "" && F.block_id != null)
            {
                string Disparam = "";

                if (F.block_id.Contains(","))
                {
                    int i = 0;
                    string[] BlockValue = F.block_id.Split(",");

                    foreach (var v in BlockValue)
                    {
                        if (i == (BlockValue.Length - 1))
                        {
                            Disparam = Disparam + "(fm.block_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fm.block_id = '" + v + "') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fm.block_id = '" + F.block_id + "')";
                }

                CommunityParam = CommunityParam + Disparam;

            }
            if (F.facility_id != "" && F.facility_id != null)
            {
                string Disparam = "";

                if (F.facility_id.Contains(","))
                {

                    int i = 0;

                    string[] FacilityValue = F.facility_id.Split(",");

                    foreach (var v in FacilityValue)
                    {
                        if (i == (FacilityValue.Length - 1))
                        {
                            Disparam = Disparam + "(fm.facility_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fm.facility_id = '" + v + "') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fm.facility_id = '" + F.facility_id + "')";
                }



                CommunityParam = CommunityParam + Disparam;
            }
            if (F.indistrict_id != "" && F.indistrict_id != null)
            {
                string Disparam = "";

                if (F.indistrict_id.Contains(","))
                {
                    int i = 0;

                    string[] indistrictValue = F.indistrict_id.Split(",");

                    foreach (var v in indistrictValue)
                    {
                        if (i == (indistrictValue.Length - 1))
                        {
                            Disparam = Disparam + "(FR.district_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.district_id = '" + v + "') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.district_id = '" + F.indistrict_id + "')";
                }

                InstitutionParam = Disparam;

            }
            if (F.inhud_id != "" && F.inhud_id != null)
            {

                string Disparam = "";

                if (F.inhud_id.Contains(","))
                {
                    int i = 0;

                    string[] inhudValue = F.inhud_id.Split(",");

                    foreach (var v in inhudValue)
                    {
                        if (i == (inhudValue.Length - 1))
                        {
                            Disparam = Disparam + "(FR.hud_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.hud_id = '" + v + "') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.hud_id = '" + F.inhud_id + "')";
                }


                InstitutionParam = InstitutionParam + Disparam;
            }
            if (F.inblock_id != "" && F.inblock_id != null)
            {
                string Disparam = "";

                if (F.inblock_id.Contains(","))
                {

                    int i = 0;

                    string[] inblockValue = F.inblock_id.Split(",");

                    foreach (var v in inblockValue)
                    {
                        if (i == (inblockValue.Length - 1))
                        {
                            Disparam = Disparam + "(FR.block_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.block_id = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.block_id = '" + F.inblock_id + "')";
                }

                InstitutionParam = InstitutionParam + Disparam;
            }
            if (F.infacility_id != "" && F.infacility_id != null)
            {
                string Disparam = "";

                if (F.infacility_id.Contains(","))
                {
                    int i = 0;

                    string[] infacilityValue = F.infacility_id.Split(",");

                    foreach (var v in infacilityValue)
                    {
                        if (i == (infacilityValue.Length - 1))
                        {
                            Disparam = Disparam + "(FR.facility_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.facility_id = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.facility_id = '" + F.infacility_id + "')";
                }

                InstitutionParam = InstitutionParam + Disparam;
            }
            if (F.directorate_id != "" && F.directorate_id != null)
            {
                string Disparam = "";

                if (F.directorate_id.Contains(","))
                {
                    int i = 0;

                    string[] indirectorateValue = F.directorate_id.Split(",");

                    foreach (var v in indirectorateValue)
                    {
                        if (i == (indirectorateValue.Length - 1))
                        {
                            Disparam = Disparam + "(FR.directorate_id = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.directorate_id = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.directorate_id = '" + F.directorate_id + "')";
                }


                InstitutionParam = InstitutionParam + Disparam;
            }
            if (F.role != "" && F.role != null)
            {

                string Disparam = "";

                if (F.role.Contains(","))
                {
                    int i = 0;

                    string[] inroleValue = F.role.Split(",");

                    foreach (var v in inroleValue)
                    {
                        if (i == (inroleValue.Length - 1))
                        {
                            Disparam = Disparam + "(UM.role = '" + v + "')";
                        }
                        else
                        {
                            Disparam = Disparam + "(UM.role = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (UM.role = '" + F.role + "')";
                }

                InstitutionParam = InstitutionParam + Disparam;
            }
        }
        private string Percentage_Cal(int Total, int Value)
        {
            double Male_Per = 0;
            Male_Per = (double.Parse(Value.ToString()) * 100 / double.Parse(Total.ToString()));
            if (Male_Per == Double.NaN || Double.IsNaN(Male_Per))
                return "0.00";
            else
                return Male_Per.ToString("F");
        }
    }
}
