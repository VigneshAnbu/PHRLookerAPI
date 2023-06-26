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
using PHRLockerAPI.Models.popDashboardkpi;
using PHRLockerAPI.ViewModel;
using System.Data;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.Intrinsics.Arm;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using static System.Net.Mime.MediaTypeNames;

namespace PHRLockerAPI.Controllers
{
    public class PopulationController : Controller
    {

        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        private readonly DapperContext _context;
        private readonly DapperContext context;

        public PopulationController(DapperContext context, IConfiguration configuration)
        {
            this.context = context;
            _configuration = configuration;
            _context = context;
        }

        string CommunityParam = "";

        string InstitutionParam = "";
        public IActionResult Index()
        {
            return View();
        }
        string fdate = "2000-01-01";
        string tdate = "2040-12-31";

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        
        [Route("getwebpopulationpop")]
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
        [Route("getwebagewisepop")]
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
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationTotalFamilyAdded")]

        public async Task<IEnumerable<Object>> GetPopulationTotalFamilyAdded()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "Select count(family_id) from family_master";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationTotalMembersAdded")]

        public async Task<IEnumerable<Object>> GetPopulationTotalMembersAdded()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "select count(member_id)from family_member_master";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationNoofDisabilityBeneficiary")]

        public async Task<IEnumerable<Object>> GetPopulationNoofDisabilityBeneficiary()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "select count(medical_history_id) from health_history where disability=true";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationCMCHISBeneficiaries")]

        public async Task<IEnumerable<Object>> GetPopulationCMCHISBeneficiaries()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "select count(family_id) from family_master where family_insurances is null";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationCongenitalAnomaly")]

        public async Task<IEnumerable<Object>> GetPopulationCongenitalAnomaly()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "select count(medical_history_id) from health_history where congenital_anomaly='True'";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationNoofIndividualslinkedwithAadhar")]

        public async Task<IEnumerable<Object>> GetPopulationNoofIndividualslinkedwithAadhar()
        {

            //Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "select count(member_id)from family_member_master where aadhaar_number is not null";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<Object>(query);
                return OBJ.ToList();
            }
        }


        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationDistrictWise")]
        public async Task<List<PopulationdistrictModel>> GetPopulationDistrictWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            PopulationdistrictModel VM = new PopulationdistrictModel();

            string query = "select * from public.GetKPIDistrictWise()";


            List<PopulationdistrictModel> RList = new List<PopulationdistrictModel>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<PopulationdistrictModel>(query);
                foreach (var result in results)
                {
                    RList.Add(result);
                }
            }
            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select gender,count(member_id),district_id from family_member_master where gender='Male' and district_id is not null group by gender,district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].male_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "select gender,count(member_id),district_id from family_member_master where gender='Female' and district_id is not null group by gender,district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].female_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "select gender,count(member_id),district_id from family_member_master where gender='Other' and district_id is not null group by gender,district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].other_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(member_id),district_id from family_member_master where resident_status_details->>'resident_details'='Verified' and district_id is not null group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].verified_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(member_id),district_id from family_member_master where aadhaar_number is not null and district_id is not null group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].aadharlinkedmembers = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(medical_history_id),FM.district_id from health_history H inner join family_master FM on FM.family_id=H.family_id where disability_details->>'udid' is not null and FM.district_id is not null group by FM.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].citizenwithudid = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(medical_history_id),FM.district_id from health_history H inner join family_master FM on FM.family_id=H.family_id where disability=true and FM.district_id is not null group by FM.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].total_disability = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(family_id),district_id from family_master where family_insurances is null and district_id is not null group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].cmchis = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(family_id),district_id from family_master where district_id is not null group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].total_families = drInner["count"].ToString();
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

                cmdInner.CommandText = "select count(member_id),district_id from family_member_master where district_id is not null group by district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].membersadded = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();



            return RList;
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

    }
}
