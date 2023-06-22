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
using System.Linq.Expressions;
using System.Runtime.Intrinsics.Arm;
using System.Security.Authentication.ExtendedProtection;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using static System.Net.Mime.MediaTypeNames;

namespace PHRLockerAPI.Controllers
{
    public class uchperformanceController : Controller
    {

        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        private readonly DapperContext context;

        public uchperformanceController(DapperContext context, IConfiguration configuration)
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
        [Route("uhcfacility")]
        public uhcperformanceModel uhcfacility([FromQuery] FilterpayloadModel F)
        {
            uhcperformanceModel uhcperfo = new uhcperformanceModel();
            List<uhcperformanceModel> uhcmonitoringreports = new List<uhcperformanceModel>();
            uhcmonitoringreports.Add(new uhcperformanceModel
            {
                typename = "Rural HSC",
                labcount = "0",
                drugcount = "0",
                screeningcount = "0"
            });
            uhcmonitoringreports.Add(new uhcperformanceModel
            {
                typename = "Rural PHC",
                labcount = "0",
                drugcount = "0",
                screeningcount = "0"
            });
            uhcmonitoringreports.Add(new uhcperformanceModel
            {
                typename = "Urban HSC",
                labcount = "0",
                drugcount = "0",
                screeningcount = "0"
            });
            uhcmonitoringreports.Add(new uhcperformanceModel
            {
                typename = "Urban PHC",
                labcount = "0",
                drugcount = "0",
                screeningcount = "0"
            });
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcmonitoringreport VM = new uhcmonitoringreport();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "select CASE WHEN block_type='Rural Block' THEN 'Rural' ELSE 'Urban'     END blocktype,facility_level,cast(sum(TotalCount) as bigint) totalcount from (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id=hh2.member_id WHERE b.drugs!='null' group by ARRUSER) tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID INNER JOIN address_block_master abm ON FR.block_id = abm.block_id where facility_level='PHC' or facility_level='HSC' group by blocktype,facility_level";
            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<districtscreeningcountmodel> RList = new List<districtscreeningcountmodel>();

            while (dr.Read())
            {
                string typename = dr["blocktype"].ToString() + " " + dr["facility_level"].ToString();
                foreach (var aa in uhcmonitoringreports)
                {
                    if (aa.typename == typename)
                    {
                        aa.drugcount = dr["totalcount"].ToString();
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
                cmdInner.CommandText = "select CASE WHEN block_type = 'Rural Block' THEN 'Rural' ELSE 'Urban' END blocktype, facility_level, count(facility_id) totalcount from FACILITY_REGISTRY fr inner join address_block_master abm on fr.block_id = abm.block_id where facility_level = 'PHC' or facility_level = 'HSC' group by blocktype,facility_level";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.typename == typename)
                        {
                            aa.facilitycount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select CASE     WHEN block_type='Rural Block' THEN 'Rural'     ELSE 'Urban'     END blocktype,facility_level,cast(sum(TotalCount) as bigint) totalcount from     (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount             FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id=hh2.member_id WHERE b.lab_test!='null' group by ARRUSER) tbl    INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)    INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID    INNER JOIN address_block_master abm ON FR.block_id = abm.block_id    where facility_level='PHC' or facility_level='HSC'    group by blocktype,facility_level";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.typename == typename)
                        {
                            aa.labcount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select CASE     WHEN block_type='Rural Block' THEN 'Rural'     ELSE 'Urban'     END blocktype,facility_level,cast(sum(TotalCount) as bigint) totalcount from     (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount             FROM Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id inner join health_history hh2 on b.member_id=hh2.member_id  group by ARRUSER) tbl    INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)    INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID    INNER JOIN address_block_master abm ON FR.block_id = abm.block_id    where facility_level='PHC' or facility_level='HSC'    group by blocktype,facility_level";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.typename == typename)
                        {
                            aa.screeningcount = drInner["totalcount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            foreach (var aa in uhcmonitoringreports)
            {
                aa.avgdrugcount = averagecalculator(double.Parse(aa.drugcount), double.Parse(aa.facilitycount));
                aa.avglabcount = averagecalculator(double.Parse(aa.labcount), double.Parse(aa.facilitycount));
                aa.avgscreeningcount = averagecalculator(double.Parse(aa.screeningcount), double.Parse(aa.facilitycount));
            }
            uhcperfo.druglist = uhcmonitoringreports;
            return uhcperfo;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("uhcfacilityservicewise")]
        public List<uhcperformanceModel> uhcfacilityservicewise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcperformanceModel VM = new uhcperformanceModel();
            List<uhcperformanceModel> vmlist = new List<uhcperformanceModel>();
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;


            cmd.CommandText = "select tbl2.*,service_name from (select JSONB_ARRAY_ELEMENTS(cast(obj->>'disease_list' as jsonb))->>'id' as symtomid,  facility_type_name,tbl.gender,count(member_id) totalcount from(select JSONB_ARRAY_ELEMENTS(diseases) obj,  JSONB_ARRAY_ELEMENTS(diseases)->> 'outcome' as aera,  b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,b.member_id,gender from health_screening  b inner join family_member_master fmm on b.member_id=fmm.member_id where JSONB_TYPEOF(diseases) = 'array' ) tbl     INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)    INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID    inner join facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id where tbl.aera!='null' group by symtomid,facility_type_name,tbl.gender) tbl2 inner join health_diagnosis_master hdm on cast(hdm.diagnosis_id as text)=tbl2.symtomid";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMOPDashboardFacility> RList = new List<VMOPDashboardFacility>();

            while (dr.Read())
            {
                int lisindex = vmlist.FindIndex(c => c.servicename == dr["service_name"].ToString() && c.typename == dr["facility_type_name"].ToString());
                if (lisindex < 0)
                {
                    lisindex = vmlist.Count;
                    vmlist.Add(new uhcperformanceModel
                    {
                        servicename = dr["service_name"].ToString(),
                        typename = dr["facility_type_name"].ToString(),
                        screeningcount = "0"
                    }); ;
                }
                if (dr["gender"].ToString() == "Male")
                {
                    vmlist[lisindex].malecount = dr["totalcount"].ToString();
                }
                else if (dr["gender"].ToString() == "Female")
                {
                    vmlist[lisindex].femalecount = dr["totalcount"].ToString();
                }
                vmlist[lisindex].screeningcount = (double.Parse(vmlist[lisindex].screeningcount) + double.Parse(dr["totalcount"].ToString())).ToString();

            }

            con.Close();



            //uhcperfo.druglist = uhcmonitoringreports;
            return vmlist;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("uhcfacilitydistrict")]
        public uhcperformanceModel uhcfacilitydistrict([FromQuery] FilterpayloadModel F)
        {
            uhcperformanceModel uhcperfo = new uhcperformanceModel();
            List<uhcperformanceModel> uhcmonitoringreports = new List<uhcperformanceModel>();

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcmonitoringreport VM = new uhcmonitoringreport();

            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select * from address_district_master ";
            NpgsqlDataReader dr = cmd.ExecuteReader();

            while (dr.Read())
            {


                uhcmonitoringreports.Add(new uhcperformanceModel
                {
                    d_name = dr["district_name"].ToString(),
                    d_id = dr["district_id"].ToString(),
                    d_gid = dr["district_gid"].ToString(),
                    drugcount = "0",
                    screeningcount = "0",
                    labcount = "0",
                    facilitycount = "0",
                });

            }
            con.Close();


            con.Open();

            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select fr.district_id,count(facility_id) totalcount from FACILITY_REGISTRY fr inner join address_block_master abm on fr.block_id = abm.block_id where facility_level = 'PHC' or facility_level = 'HSC' group by fr.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {

                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.d_id == drInner["district_id"].ToString())
                        {
                            aa.facilitycount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.district_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.district_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE b.drugs!='null'  and CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.district_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    //string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.d_id == drInner["district_id"].ToString())
                        {
                            aa.drugcount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.district_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.district_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE b.lab_test!='null'  and CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.district_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    // string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.d_id == drInner["district_id"].ToString())
                        {
                            aa.labcount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.district_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.district_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.district_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.d_id == drInner["district_id"].ToString())
                        {
                            aa.screeningcount = drInner["totalcount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            foreach (var aa in uhcmonitoringreports)
            {
                aa.avgdrugcount = averagecalculator(double.Parse(aa.drugcount), double.Parse(aa.facilitycount));
                aa.avglabcount = averagecalculator(double.Parse(aa.labcount), double.Parse(aa.facilitycount));
                aa.avgscreeningcount = averagecalculator(double.Parse(aa.screeningcount), double.Parse(aa.facilitycount));
            }
            uhcperfo.druglist = uhcmonitoringreports;
            return uhcperfo;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("uhcfacilityhud")]
        public uhcperformanceModel uhcfacilityhud([FromQuery] FilterpayloadModel F)
        {
            uhcperformanceModel uhcperfo = new uhcperformanceModel();
            List<uhcperformanceModel> uhcmonitoringreports = new List<uhcperformanceModel>();

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            uhcmonitoringreport VM = new uhcmonitoringreport();

            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select adm.district_id,district_name,district_gid,hud_name,hud_id,hud_gid from address_district_master adm inner join address_hud_master ahm on adm.district_id=ahm.district_id ";
            NpgsqlDataReader dr = cmd.ExecuteReader();

            while (dr.Read())
            {


                uhcmonitoringreports.Add(new uhcperformanceModel
                {
                    h_id = dr["hud_id"].ToString(),
                    h_gid = dr["hud_gid"].ToString(),
                    h_name = dr["hud_name"].ToString(),
                    d_name = dr["district_name"].ToString(),
                    d_id = dr["district_id"].ToString(),
                    d_gid = dr["district_gid"].ToString(),
                    drugcount = "0",
                    screeningcount = "0",
                    labcount = "0",
                    facilitycount = "0",
                });

            }
            con.Close();


            con.Open();

            if (1 == 1)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                cmdInner.CommandText = "select fr.hud_id,count(facility_id) totalcount from FACILITY_REGISTRY fr inner join address_block_master abm on fr.block_id = abm.block_id where facility_level = 'PHC' or facility_level = 'HSC' group by fr.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {

                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.h_id == drInner["hud_id"].ToString())
                        {
                            aa.facilitycount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.hud_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.hud_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE b.drugs!='null'  and CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.hud_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    //string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.h_id == drInner["hud_id"].ToString())
                        {
                            aa.drugcount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.hud_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.hud_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE b.lab_test!='null'  and CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.hud_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    // string typename = drInner["blocktype"].ToString() + " " + drInner["facility_level"].ToString();
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.h_id == drInner["hud_id"].ToString())
                        {
                            aa.labcount = drInner["totalcount"].ToString();
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
                cmdInner.CommandText = "select tbl.hud_id,cast(sum(TotalCount) as bigint) totalcount from  (SELECT b.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,count(b.member_id) TotalCount,fm.hud_id FROM   Health_screening b INNER JOIN family_master fm ON b.family_id = fm.family_id   inner join health_history hh2 on b.member_id=hh2.member_id   WHERE CURRENT_DATE + interval '-290' day < b.last_update_date  group by ARRUSER,fm.hud_id) tbl   INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)   INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   INNER JOIN address_block_master abm ON FR.block_id = abm.block_id   where facility_level='PHC' or facility_level='HSC'  group by tbl.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                districtscreeningcountmodel SList = new districtscreeningcountmodel();
                while (drInner.Read())
                {
                    foreach (var aa in uhcmonitoringreports)
                    {
                        if (aa.h_id == drInner["hud_id"].ToString())
                        {
                            aa.screeningcount = drInner["totalcount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            foreach (var aa in uhcmonitoringreports)
            {
                aa.avgdrugcount = averagecalculator(double.Parse(aa.drugcount), double.Parse(aa.facilitycount));
                aa.avglabcount = averagecalculator(double.Parse(aa.labcount), double.Parse(aa.facilitycount));
                aa.avgscreeningcount = averagecalculator(double.Parse(aa.screeningcount), double.Parse(aa.facilitycount));
            }
            uhcperfo.druglist = uhcmonitoringreports;
            return uhcperfo;
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
        private string averagecalculator(double Total, double Value)
        {
            if (Total == 0)
            {
                return "0.00";
            }
            double Male_Per = 0;
            //Male_Per = (double.Parse(Value.ToString()) * 100 / double.Parse(Total.ToString()));
            Male_Per = (double.Parse(Total.ToString()) / double.Parse(Value.ToString()));
            if (Male_Per == Double.NaN || Double.IsNaN(Male_Per))
                return "0.00";
            else
                return Male_Per.ToString("F");
        }
    }
}
