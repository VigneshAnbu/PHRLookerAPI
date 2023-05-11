using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using PHRLockerAPI.Intfa;
using PHRLockerAPI.Models;
using PHRLockerAPI.ViewModel;
using System;
using System.Data;
using System.Net;

namespace PHRLockerAPI.Controllers
{
    public class SyncphrhimsController : ControllerBase
    {
        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        int Check = 0;

        public SyncphrhimsController(IConfiguration configuration, Ismsgateway ismsgateway)
        {
            _configuration = configuration;
            _ismsgateway = ismsgateway;
        }

        //private string GetIPAddress()
        //{
        //    string IPAddress = "";
        //    IPHostEntry Host = default(IPHostEntry);
        //    string Hostname = null;
        //    Hostname = System.Environment.MachineName;
        //    Host = Dns.GetHostEntry(Hostname);
        //    foreach (IPAddress IP in Host.AddressList)
        //    {
        //        if (IP.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
        //        {
        //            IPAddress = Convert.ToString(IP);
        //        }
        //    }
        //    return IPAddress;
        //}

        //private void checkingipaddress()
        //{

        //    string IPAddress = GetIPAddress();

        //    if (IPAddress == "135.181.219.108")
        //        Check = 1;
        //    else
        //        Check = 0;
        //}

        [Authorize]
        [HttpPost]
        [Route("SyncGetPHRID")]
        public List<SyncPHRIDModel> GetPHRID(string Param, string Type)
        {

            //IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName()); // `Dns.Resolve()` method is deprecated.
            //IPAddress ipAddress = ipHostInfo.AddressList[0];

            //string dtr= ipAddress.ToString();
            List<SyncPHRIDModel> RList = new List<SyncPHRIDModel>();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return RList;
            //}

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            string Pass = "";

            if (Type == "R")
            {
                Pass = "where (F.pds_smart_card_id='" + Param + "')";
            }
            else if (Type == "A")
            {
                Pass = "where (M.aadhaar_number'" + Param + "')";
            }
            else if (Type == "M")
            {
                Pass = "where (M.mobile_number='" + Param + "')";
            }

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select unique_health_id,member_name,gender,date_part('year',age(birth_date)) age,pds_smart_card_id,mobile_number from family_member_master M inner join family_master F on F.family_id=M.family_id  " + Pass + " group by unique_health_id,member_name,gender,age,pds_smart_card_id,mobile_number";
            con.Open();
            NpgsqlDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                var SList = new SyncPHRIDModel();
                SList.member_name = dr["member_name"].ToString();
                SList.unique_health_id = dr["unique_health_id"].ToString();
                SList.age = dr["age"].ToString();
                SList.gender = dr["gender"].ToString();
                SList.rationcard = dr["pds_smart_card_id"].ToString();
                SList.mobilenumber = dr["mobile_number"].ToString();


                RList.Add(SList);
            }


            con.Close();

            return RList;
        }

        [Authorize]
        [HttpPost]
        [Route("SyncUpdatingHIMSID")]
        public IActionResult UpdatingHIMSID(string PHRID, string HIMSID)
        {
            ResponseModel R = new ResponseModel();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return Ok(R);
            //}


            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select unique_health_id from family_member_master where unique_health_id='" + PHRID + "'";
            con.Open();
            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<SyncPHRIDModel> RList = new List<SyncPHRIDModel>();

            while (dr.Read())
            {

                var SList = new SyncPHRIDModel();

                SList.unique_health_id = dr["unique_health_id"].ToString();

                RList.Add(SList);
            }

            con.Close();



            con.Open();

            if (RList.Count > 0)
            {
                NpgsqlCommand cmdUpdate = new NpgsqlCommand();
                cmdUpdate.Connection = con;
                cmdUpdate.CommandType = CommandType.Text;
                cmdUpdate.CommandText = "update family_member_master set hims_id='" + HIMSID + "' where unique_health_id='" + PHRID + "'";
                NpgsqlDataReader drUpdate = cmdUpdate.ExecuteReader();

                R.ResponseCode = "1";
                R.ResponseMessage = "HIMS ID Mapped with PHR ID";
            }
            else
            {
                R.ResponseCode = "2";
                R.ResponseMessage = "No Records Found";
            }


            con.Close();

            return Ok(R);
        }


        [Authorize]
        [HttpPost]
        [Route("SyncNewUser")]
        public IActionResult CreateNewUser(NewUserModel U)
        {
            ResponseModel R = new ResponseModel();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return Ok(R);
            //}

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();

            NpgsqlCommand cmdUpdate = new NpgsqlCommand();
            cmdUpdate.Connection = con;
            cmdUpdate.CommandType = CommandType.Text;
            cmdUpdate.CommandText = "insert into family_member_tempnew(member_name,gender,mobile_number,dob,street_name,block_name,district_name,himsid)values('" + U.member_name + "','" + U.gender + "','" + U.mobile_number + "','" + U.dob + "','" + U.street_name + "','" + U.block_name + "','" + U.district_name + "','" + U.himsid + "')";
            NpgsqlDataReader drUpdate = cmdUpdate.ExecuteReader();

            con.Close();

            R.ResponseCode = "1";
            R.ResponseMessage = "New User Created";

            return Ok(R);
        }

        [Authorize]
        [HttpPost]
        [Route("SyncGetPHRIDusingHIMSID")]
        public IActionResult GetPHRIDusingHIMSID(string HIMSID)
        {
            ResponseModel R = new ResponseModel();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return Ok(R);
            //}

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select unique_health_id from family_member_master where himsid='" + HIMSID + "'";
            con.Open();
            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<SyncPHRIDModel> RList = new List<SyncPHRIDModel>();

            while (dr.Read())
            {

                var SList = new SyncPHRIDModel();

                SList.unique_health_id = dr["unique_health_id"].ToString();

                R.Value = SList.unique_health_id;

                RList.Add(SList);
            }

            if (RList.Count > 0)
            {
                R.ResponseCode = "1";
                R.ResponseMessage = "PHR ID";
            }
            else
            {
                R.ResponseCode = "2";
                R.ResponseMessage = "No Records";
            }

            con.Close();


            return Ok(R);
        }

        [Authorize]
        [HttpPost]
        [Route("SyncGetScreeningvalues")]
        public List<ScreeningModel> GetScreeningvalues(string PHRID)
        {
            ResponseModel R = new ResponseModel();

            List<ScreeningModel> RList = new List<ScreeningModel>();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return RList;
            //}

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select screening_values->>'bmi' bmi,screening_values->>'breathe_difficulty' breathe_difficulty,screening_values->>'chestpain' chestpain,screening_values->>'dia_bp' dia_bp,screening_values->>'dim_vision' dim_vision,screening_values->>'dizziness' dizziness,screening_values->>'dm_risk_score' dm_risk_score,screening_values->>'dm_screening' dm_screening,screening_values->>'fatigue' fatigue,screening_values->>'freq_urine' freq_urine,screening_values->>'height' height,screening_values->>'hip_circumference' hip_circumference,screening_values->>'ht_screening' ht_screening,screening_values->>'nota_diabetes' nota_diabetes,screening_values->>'nota_htn' nota_htn,screening_values->>'palpitation' palpitation,screening_values->>'pulse_rate' pulse_rate,screening_values->>'rbs' rbs,screening_values->>'rbs_date' rbs_date,screening_values->>'rr' rr,screening_values->>'spo2' spo2,screening_values->>'sys_bp' sys_bp,screening_values->>'temp' tempr,screening_values->>'thirsty' thirsty,screening_values->>'waist_circumference' waist_circumference,screening_values->>'waist_hip_ratio' waist_hip_ratio,screening_values->>'weight' weight from health_Screening S inner join family_member_master M on M.member_id=S.member_id where M.unique_health_id='" + PHRID + "'";

            con.Open();
            NpgsqlDataReader dr = cmd.ExecuteReader();


            while (dr.Read())
            {

                var SList = new ScreeningModel();

                SList.bmi = dr["bmi"].ToString();
                SList.breathe_difficulty = dr["breathe_difficulty"].ToString();
                SList.chestpain = dr["chestpain"].ToString();
                SList.dia_bp = dr["dia_bp"].ToString();
                SList.dim_vision = dr["dim_vision"].ToString();
                SList.dizziness = dr["dizziness"].ToString();
                SList.dm_risk_score = dr["dm_risk_score"].ToString();
                SList.dm_screening = dr["dm_screening"].ToString();
                SList.fatigue = dr["fatigue"].ToString();
                SList.freq_urine = dr["freq_urine"].ToString();
                SList.height = dr["height"].ToString();
                SList.hip_circumference = dr["hip_circumference"].ToString();
                SList.ht_screening = dr["ht_screening"].ToString();
                SList.nota_diabetes = dr["nota_diabetes"].ToString();
                SList.nota_htn = dr["nota_htn"].ToString();
                SList.palpitation = dr["palpitation"].ToString();
                SList.pulse_rate = dr["pulse_rate"].ToString();
                SList.rbs = dr["rbs"].ToString();
                SList.rbs_date = dr["rbs_date"].ToString();
                SList.rr = dr["rr"].ToString();
                SList.spo2 = dr["spo2"].ToString();
                SList.sys_bp = dr["sys_bp"].ToString();
                SList.tempr = dr["tempr"].ToString();
                SList.thirsty = dr["thirsty"].ToString();
                SList.waist_circumference = dr["waist_circumference"].ToString();
                SList.waist_hip_ratio = dr["waist_hip_ratio"].ToString();
                SList.weight = dr["weight"].ToString();

                RList.Add(SList);
            }

            con.Close();


            return RList;
        }

        [Authorize]
        [HttpPost]
        [Route("SyncGetHIMSNewUserList")]
        public List<NewUserModel> GetHIMSNewUserList()
        {
            ResponseModel R = new ResponseModel();

            List<NewUserModel> RList = new List<NewUserModel>();

            //checkingipaddress();

            //if (Check != 1)
            //{
            //    return RList;
            //}


            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select * from family_member_tempnew";

            con.Open();
            NpgsqlDataReader dr = cmd.ExecuteReader();


            while (dr.Read())
            {

                var SList = new NewUserModel();

                SList.member_name = dr["member_name"].ToString();
                SList.mobile_number = dr["mobile_number"].ToString();
                SList.gender = dr["gender"].ToString();
                SList.mobile_number = dr["mobile_number"].ToString();
                SList.dob = dr["dob"].ToString();
                SList.street_name = dr["street_name"].ToString();
                SList.block_name = dr["block_name"].ToString();
                SList.district_name = dr["district_name"].ToString();
                SList.himsid = dr["himsid"].ToString();

                RList.Add(SList);
            }

            con.Close();


            return RList;
        }


    }
}
