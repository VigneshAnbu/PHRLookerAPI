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
    public class uchmonitoringController : Controller
    {

        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        private readonly DapperContext context;

        public uchmonitoringController(DapperContext context, IConfiguration configuration)
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
        [Route("uhcmonitoringht")]
        public uhcmonitoringreport uhcmonitoringht([FromQuery] FilterpayloadModel F)
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
                VM.hypertesion= dr["totalcount"].ToString();
            }
            con.Close();
            con.Open();

            if (1==1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "SELECT * from public.uhcgethypertensionsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    VM.htsuspected= drInner["totalcount"].ToString();
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
        [Route("uhcmonitoringdm")]
        public uhcmonitoringreport uhcmonitoringdm([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcmonitoringreport VM = new uhcmonitoringreport();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;


            cmd.CommandText = "SELECT * from public.uhcgetdm('" + CommunityParam + "','" + InstitutionParam + "')";


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
                cmdInner.CommandText = "SELECT * from public.uhcgetdmsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

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
                cmdInner.CommandText = "SELECT * from public.uhcgetdmsuspectedconfirmed('" + CommunityParam + "','" + InstitutionParam + "')";

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
                cmdInner.CommandText = "SELECT * from public.uhcgetdmdrug('" + CommunityParam + "','" + InstitutionParam + "')";

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
        [Route("uhcmonitoringdistrict")]
        public districtscreeningcountmodel uhcmonitoringdistrict([FromQuery] FilterpayloadModel F)
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

            cmd.CommandText = "SELECT * from public.uhcgetdistrictpopulation('" + CommunityParam + "')";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<districtscreeningcountmodel> RList = new List<districtscreeningcountmodel>();

            while (dr.Read())
            {
                var SList = new districtscreeningcountmodel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_id = dr["district_id"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();
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



                //cmdInner.CommandText = "select tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END AgeText,count(member_id) TotalCount from ( select MS.district_name,MS.district_gid,date_part('year',age(birth_date)) Age,M.member_id from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  inner join public.health_screening as S  on M.member_id=S.member_id  group by MS.district_name,MS.district_gid,date_part('year',age(birth_date)),M.member_id) tbl group by tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END";

                cmdInner.CommandText = "SELECT * from public.uhcgetdistrictabove30()";



                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_id = RList[i].district_id;
                        if (SList.district_id == drInner["district_id"].ToString())
                        {
                            
                            RList[i].above30 = drInner["totalcount"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.uhcgetdistrictabove30screening('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n group by fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n inner join address_district_master dm on tbl.district_id=dm.district_id \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
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
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT * from public.uhcgetdistricthtsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].suspected = drInner["TotalCount"].ToString();
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


                cmdInner.CommandText = "SELECT * from public.uhcgetdistricthtsuspectedconf('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].suspectedconfirmed = drInner["TotalCount"].ToString();

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


                cmdInner.CommandText = "SELECT * from public.uhcgetdistrictdrug('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].drugissued  = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            for (int i = 0; i < RList.Count; i++)
            {
                RList[i].scrpercentage = Percentage_Cal(int.Parse(RList[i].above30),int.Parse(RList[i].screeningCount));
                //RList[i].multiscreening = (int.Parse(RList[i].uniqueCount) - int.Parse(RList[i].onescreening)).ToString();
            }
            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("uhcmonitoringhud")]
        public hudscreeningcountmodel uhcmonitoringhud([FromQuery] FilterpayloadModel F)
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

            cmd.CommandText = "SELECT * from public.uhcgethudpopulation('" + CommunityParam + "')";


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
                SList.TotalCount = dr["TotalCount"].ToString();
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

                //cmdInner.CommandText = "select tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END AgeText,count(member_id) TotalCount from ( select MS.district_name,MS.district_gid,date_part('year',age(birth_date)) Age,M.member_id from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  inner join public.health_screening as S  on M.member_id=S.member_id  group by MS.district_name,MS.district_gid,date_part('year',age(birth_date)),M.member_id) tbl group by tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END";

                cmdInner.CommandText = "SELECT * from public.uhcgethudabove30()";



                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {

                            RList[i].above30 = drInner["totalcount"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.uhcgethudabove30screening('" + CommunityParam + "','" + InstitutionParam + "')";

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
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT * from public.uhcgethudhtsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].suspected = drInner["TotalCount"].ToString();
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


                cmdInner.CommandText = "SELECT * from public.uhcgethudhtsuspectedconf('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

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
                            RList[i].suspectedconfirmed = drInner["TotalCount"].ToString();

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


                cmdInner.CommandText = "SELECT * from public.uhcgethuddrug('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].drugissued = drInner["TotalCount"].ToString();

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
        [Route("uhcmonitoringblock")]
        public blockscreeningcountmodel uhcmonitoringblock([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            blockscreeningcountmodel VM = new blockscreeningcountmodel();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  group by MS.district_name,MS.district_gid";

            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(fm.family_id) TotalCount from  public.address_district_master as MS  \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id  " + CommunityParam + " \r\ngroup by MS.district_name,MS.district_gid";

            cmd.CommandText = "SELECT * from public.uhcgetblockpopulation('" + CommunityParam + "')";


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
                SList.TotalCount = dr["TotalCount"].ToString();
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

                //cmdInner.CommandText = "select tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END AgeText,count(member_id) TotalCount from ( select MS.district_name,MS.district_gid,date_part('year',age(birth_date)) Age,M.member_id from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  inner join public.health_screening as S  on M.member_id=S.member_id  group by MS.district_name,MS.district_gid,date_part('year',age(birth_date)),M.member_id) tbl group by tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END";

                cmdInner.CommandText = "SELECT * from public.uhcgetblockabove30()";



                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {

                            RList[i].above30 = drInner["totalcount"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.uhcgetblockabove30screening('" + CommunityParam + "','" + InstitutionParam + "')";

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
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT * from public.uhcgetblockhtsuspected('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].suspected = drInner["TotalCount"].ToString();
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


                cmdInner.CommandText = "SELECT * from public.uhcgetblockhtsuspectedconf('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

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
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;


                cmdInner.CommandText = "SELECT * from public.uhcgetblockdrug('" + CommunityParam + "','" + InstitutionParam + "')";

                //cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].drugissued = drInner["TotalCount"].ToString();

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
