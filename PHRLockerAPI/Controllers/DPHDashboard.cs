using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using PHRLockerAPI.DBContext;
using PHRLockerAPI.Models;
using PHRLockerAPI.Models.popDashboardkpi;
using PHRLockerAPI.ViewModel;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.Intrinsics.Arm;
using System.Reflection;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using static System.Net.Mime.MediaTypeNames;
using Microsoft.CodeAnalysis.Semantics;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;


using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;

using PHRLockerAPI;
using PHRLockerAPI.DBContext;
//using PHRLockerAPI.DBContext;
using PHRLockerAPI.Intfa;
using PHRLockerAPI.Dto;
using PHRLockerAPI.Models.MtmBenfModel;
using Microsoft.OpenApi.Writers;
using Microsoft.Extensions.FileSystemGlobbing.Internal;


namespace PHRLockerAPI.Controllers
{
    [ApiController]
    public class DPHDashboard : ControllerBase
    {
        private readonly IConfiguration _configuration;

        private readonly DapperContext _context;
        string CommunityParam = "";
        string InstitutionParam = "";

        private readonly DapperContext context;

        public DPHDashboard(DapperContext context, IConfiguration configuration)
        {
            this.context = context;
            _configuration = configuration;
            _context = context;
        }

        [HttpGet]
        [Route("FilterAll")]
        public void Filterforall(FilterpayloadModel F)
        {
            if (F.district_id != "")
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
                            Disparam = Disparam + "(fmm.district_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fmm.district_id = ''" + v + "'') or ";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fmm.district_id = ''" + F.district_id + "'')";
                }


                CommunityParam = Disparam;

            }
            if (F.hud_id != "")
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
                            Disparam = Disparam + "(fmm.hud_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fmm.hud_id = ''" + v + "'') or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fmm.hud_id = ''" + F.hud_id + "'')";
                }


                CommunityParam = CommunityParam + Disparam;
            }
            if (F.block_id != "")
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
                            Disparam = Disparam + "(fmm.block_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fmm.block_id = ''" + v + "'') or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fmm.block_id = ''" + F.block_id + "'')";
                }

                CommunityParam = CommunityParam + Disparam;

            }
            if (F.village_id != "")
            {
                string Disparam = "";

                if (F.village_id.Contains(","))
                {
                    int i = 0;
                    string[] villageValue = F.village_id.Split(",");

                    foreach (var v in villageValue)
                    {
                        if (i == (villageValue.Length - 1))
                        {
                            Disparam = Disparam + "(fmm.village_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fmm.village_id = ''" + v + "'') or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (fmm.village_id = ''" + F.village_id + "'')";
                }

                CommunityParam = CommunityParam + Disparam;

            }

        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationCount")]
        public VMGetPopulationCountModel getpopulationcount([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPopulationCountModel VM = new VMGetPopulationCountModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT COUNT(member_id) FROM family_member_master AS fmm WHERE date_part('year',age(birth_date)) >= 18 "+CommunityParam+"";
            cmdInner.CommandText = "SELECT * FROM public.getpopulationcount_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.total_population = drInner["count"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetTotalScreeningCount")]
        public VMScreeningCountModel getTotalScreeningcount([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMScreeningCountModel VM = new VMScreeningCountModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT COUNT(*)\r\nFROM address_district_master as adm\r\nINNER JOIN family_member_master as fmm ON adm.district_id = fmm.district_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\nwhere date_part('year',age(birth_date))> 18\r\n " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.gettotalscreeningcount_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.total_Screening = drInner["count"].ToString();

            }

            con.Close();

            return VM;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetConfirmedCasesCount")]
        public VMTotalConfirmedModel getConfirmedCasesCount([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMTotalConfirmedModel VM = new VMTotalConfirmedModel();

            Filterforall(F);

           /*
                string para = "";
            if (CommunityParam.StartsWith("and"))
            {
                para = Regex.Replace(CommunityParam, @"^and", "Where", RegexOptions.IgnoreCase);
            }
            else
            {
                para = CommunityParam;
            }
            */

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT \r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') + \r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') +\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') as Confirmed\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.getconfirmedcasescount_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.total_Confirmed_caeses = drInner["confirmed"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDiabetesvsHypertensionvsBoth")]
        public VMGetDiabetesvsHypertensionvsBothModel getDiabetesvsHypertensionvsBoth ([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetDiabetesvsHypertensionvsBothModel VM = new VMGetDiabetesvsHypertensionvsBothModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') +\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') +\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS hypertension,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'rbs' < '140' \r\nAND hs.screening_values->>'dm_screening' = 'Known DM' AND hs.screening_values->>'ht_screening' = 'Known HT' \r\n and ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS Both\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.getdiahyperboth_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Diabetes_Mellitus = drInner["Diabetes_Mellitus"].ToString();
                VM.Hypertension = drInner["Hypertension"].ToString();
                VM.Both = drInner["dhboth"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetConfirmedControlledUncontrolledDiabetes")]
        public VMGetConfirmControledUncontroledDiabetesModel getConfirmedControlledUncontrolledDiabetes([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetConfirmControledUncontroledDiabetesModel VM = new VMGetConfirmControledUncontroledDiabetesModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Suspected DM') AS uncontrolled_diabetes\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.getconfcontrouncontrodia_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Confirmed_Diabetes_Mellitus = drInner["confirmed_diabetes_mellitus"].ToString();
                VM.Controlled_Diabetes_Mellitus = drInner["controlled_diabetes_mellitus"].ToString();
                VM.Uncontrolled_Diabetes = drInner["uncontrolled_diabetes"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetConfirmedControlledUncontrolledHypertension")]
        public VMConfirmControledUncontroledHypertensionsModel GetConfirmedControlledUncontrolledHypertension([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMConfirmControledUncontroledHypertensionsModel VM = new VMConfirmControledUncontroledHypertensionsModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Suspected HT') AS uncontrolled_hypertension\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.getconfcontrouncontrohyper_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Confirmed_Hypertension = drInner["confirmed_hypertension"].ToString();
                VM.Controlled_Hypertension = drInner["controlled_hypertension"].ToString();
                VM.Uncontrolled_Hypertension = drInner["uncontrolled_hypertension"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetConfirmedControlledUncontrolledBoth")]
        public VMConfirmedControledUncontroledBothModel getconfirmedcontrolleduncontrolledboth([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMConfirmedControledUncontroledBothModel VM = new VMConfirmedControledUncontroledBothModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            //cmdInner.CommandText = "SELECT\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Suspected DM' and  hs.screening_values->>'ht_screening' = 'Suspected HT') AS \r\nuncontrolled_both\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + "";
            cmdInner.CommandText = "SELECT * FROM public.getconfcontrouncontroboth_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Confirmed_Both = drInner["confirmed_both"].ToString();
                VM.Controlled_Both = drInner["controlled_both"].ToString();
                VM.Uncontrolled_Both = drInner["uncontrolled_both"].ToString();

            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDistrictWisePopulation")]
        public VMGetDistrictWiseModel[] getdistrictwisepopulation([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetDistrictWiseModel VM = new VMGetDistrictWiseModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "SELECT adm.district_name,adm.district_gid, COUNT(fmm.member_id) \r\nFROM address_district_master as adm\r\nINNER JOIN family_member_master as fmm ON adm.district_id = fmm.district_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid";
            cmd.CommandText = "SELECT * FROM public.getdiswise_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetDistrictWiseModel> RList = new List<VMGetDistrictWiseModel>();

            while (drInner.Read())
            {
                var SList = new VMGetDistrictWiseModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Total_Population = drInner["count"].ToString();

                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,COUNT(*) \r\nFROM address_district_master as adm\r\nINNER JOIN family_member_master as fmm ON adm.district_id = fmm.district_id\r\nwhere date_part('year',age(fmm.birth_date))>='18'\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid";
                cmdInner.CommandText = "SELECT * FROM public.getdiswise1_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].District_Gid == drInner1["district_gid"].ToString())
                        {
                            RList[i].Population_Age = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,COUNT(hs.screening_id) \r\nFROM address_district_master as adm\r\nINNER JOIN family_member_master as fmm ON adm.district_id = fmm.district_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid";
                cmdInner.CommandText = "SELECT * FROM public.getdiswise2_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].District_Gid == drInner1["district_gid"].ToString())
                        {
                            RList[i].Total_Screening = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,\r\n  count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\n  count(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\n  COALESCE(\r\n    count(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') /\r\n    NULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM'), 0) * 100,\r\n    0\r\n  ) AS diabetes_percentage\r\nFROM\r\n  address_district_master AS adm\r\n  INNER JOIN family_member_master AS fmm ON adm.district_id = fmm.district_id\r\n  INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam +" GROUP BY adm.district_name,adm.district_gid\r\n ";
                cmdInner.CommandText = "SELECT * FROM public.getdiswise3_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].District_Gid == drInner1["district_gid"].ToString())
                        {
                            RList[i].Confirmed_Diabetes_Mellitus = drInner1["confirmed_diabetes_mellitus"].ToString();

                            RList[i].Controlled_Diabetes_Mellitus = drInner1["controlled_diabetes_mellitus"].ToString();

                            RList[i].Diabetes_Percentage = drInner1["diabetes_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,\r\n  count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\n  count(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140)\r\n    AND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\n  COALESCE(\r\n    count(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140)\r\n      AND hs.screening_values->>'ht_screening' = 'Known HT') /\r\n    NULLIF(count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,\r\n    0\r\n  ) AS hypertension_percentage\r\nFROM\r\n  address_district_master AS adm\r\n  INNER JOIN family_member_master AS fmm ON adm.district_id = fmm.district_id\r\n  INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam +" GROUP BY adm.district_name,adm.district_gid\r\n ";
                cmdInner.CommandText = "SELECT * FROM public.getdiswise4_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].District_Gid == drInner1["district_gid"].ToString())
                        {
                            RList[i].Confirmed_Hypertension = drInner1["confirmed_hypertension"].ToString();

                            RList[i].Controlled_Hypertension = drInner1["controlled_hypertension"].ToString();

                            RList[i].Hypertension_Percentage = drInner1["hypertension_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,\r\n  count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' AND hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\n  count(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' AND\r\n    ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) AND\r\n    hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\n  COALESCE(\r\n    count(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' AND\r\n      ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) AND\r\n      hs.screening_values->>'ht_screening' = 'Known HT') /\r\n    NULLIF(\r\n      count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' AND hs.screening_values->>'ht_screening' = 'Known HT'),\r\n      0\r\n    ) * 100,\r\n    0\r\n  ) AS both_percentage\r\nFROM\r\n  address_district_master AS adm\r\n  INNER JOIN family_member_master AS fmm ON adm.district_id = fmm.district_id\r\n  INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid\r\n ";
                cmdInner.CommandText = "SELECT * FROM public.getdiswise5_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].District_Gid == drInner1["district_gid"].ToString())
                        {
                            RList[i].Confirmed_Both = drInner1["confirmed_both"].ToString();

                            RList[i].Controlled_Both = drInner1["controlled_both"].ToString();

                            RList[i].Both_Percentage = drInner1["both_percentage"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetDistrictWiseModel[] RArray = RList.ToArray();

            return RArray;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetHudWisePopulation")]
        public VMGetHudWiseModel[] getHudWisePopulation([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetHudWiseModel VM = new VMGetHudWiseModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid, COUNT(fmm.member_id) \r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid";
            cmd.CommandText = "SELECT * FROM public.gethudwise_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetHudWiseModel> RList = new List<VMGetHudWiseModel>();

            while (drInner.Read())
            {
                var SList = new VMGetHudWiseModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Total_Population = drInner["count"].ToString();

                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,COUNT(*) \r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\nwhere date_part('year',age(fmm.birth_date))>='18'\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid";
                cmdInner.CommandText = "SELECT * FROM public.gethudwise1_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Hud_Gid == drInner1["hud_gid"].ToString())
                        {
                            RList[i].Population_Age = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,COUNT(*) \r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid";
                cmdInner.CommandText = "SELECT * FROM public.gethudwise2_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Hud_Gid == drInner1["hud_gid"].ToString())
                        {
                            RList[i].Total_Screening = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM'), 0) * 100,0) AS diabetes_percentage\r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n  " + CommunityParam + " Group By adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.gethudwise3_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Hud_Gid == drInner1["hud_gid"].ToString())
                        {
                            RList[i].Confirmed_Diabetes_Mellitus = drInner1["confirmed_diabetes_mellitus"].ToString();

                            RList[i].Controlled_Diabetes_Mellitus = drInner1["controlled_diabetes_mellitus"].ToString();

                            RList[i].Diabetes_Percentage = drInner1["diabetes_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) AS hypertension_percentage\r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.gethudwise4_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Hud_Gid == drInner1["hud_gid"].ToString())
                        {
                            RList[i].Confirmed_Hypertension = drInner1["confirmed_hypertension"].ToString();

                            RList[i].Controlled_Hypertension = drInner1["controlled_hypertension"].ToString();

                            RList[i].Hypertension_Percentage = drInner1["hypertension_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) \r\nAS both_percentage\r\nFROM address_hud_master as ahm\r\nINNER JOIN address_district_master as adm ON ahm.district_id = adm.district_id\r\nINNER JOIN family_member_master as fmm ON ahm.hud_id = fmm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " Group By adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.gethudwise5_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Hud_Gid == drInner1["hud_gid"].ToString())
                        {
                            RList[i].Confirmed_Both = drInner1["confirmed_both"].ToString();

                            RList[i].Controlled_Both = drInner1["controlled_both"].ToString();

                            RList[i].Both_Percentage = drInner1["both_percentage"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetHudWiseModel[] RArray = RList.ToArray();

            return RArray;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetBlockWisePopulation")]
        public VMGetBlockWiseModel[] getblockwisepopulation([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetBlockWiseModel VM = new VMGetBlockWiseModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid, COUNT(fmm.member_id) \r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid";
            cmd.CommandText = "SELECT * FROM public.getblockwise_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetBlockWiseModel> RList = new List<VMGetBlockWiseModel>();

            while (drInner.Read())
            {
                var SList = new VMGetBlockWiseModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Total_Population = drInner["count"].ToString();

                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid, COUNT(*) \r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nwhere date_part('year',age(fmm.birth_date))>='18'\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid";
                cmdInner.CommandText = "SELECT * FROM public.getblockwise1_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Block_Gid == drInner1["block_gid"].ToString())
                        {
                            RList[i].Population_Age = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid, COUNT(*) \r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid";
                cmdInner.CommandText = "SELECT * FROM public.getblockwise2_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Block_Gid == drInner1["block_gid"].ToString())
                        {
                            RList[i].Total_Screening = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM'), 0) * 100,0) AS diabetes_percentage\r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getblockwise3_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Block_Gid == drInner1["block_gid"].ToString())
                        {
                            RList[i].Confirmed_Diabetes_Mellitus = drInner1["confirmed_diabetes_mellitus"].ToString();

                            RList[i].Controlled_Diabetes_Mellitus = drInner1["controlled_diabetes_mellitus"].ToString();

                            RList[i].Diabetes_Percentage = drInner1["diabetes_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) AS hypertension_percentage\r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getblockwise4_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Block_Gid == drInner1["block_gid"].ToString())
                        {
                            RList[i].Confirmed_Hypertension = drInner1["confirmed_hypertension"].ToString();

                            RList[i].Controlled_Hypertension = drInner1["controlled_hypertension"].ToString();

                            RList[i].Hypertension_Percentage = drInner1["hypertension_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) \r\nAS both_percentage\r\nFROM address_block_master as abm\r\nINNER JOIN family_member_master as fmm ON abm.block_id = fmm.block_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getblockwise5_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Block_Gid == drInner1["block_gid"].ToString())
                        {
                            RList[i].Confirmed_Both = drInner1["confirmed_both"].ToString();

                            RList[i].Controlled_Both = drInner1["controlled_both"].ToString();

                            RList[i].Both_Percentage = drInner1["both_percentage"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetBlockWiseModel[] RArray = RList.ToArray();

            return RArray;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetVillageWisePopulation")]
        public VMGetVillageWiseModel[] GetVillageWisePopulation([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetVillageWiseModel VM = new VMGetVillageWiseModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid, COUNT(fmm.member_id) \r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid";
            cmd.CommandText = "SELECT * FROM public.getvillagewise_dph(' " + CommunityParam + " ')";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetVillageWiseModel> RList = new List<VMGetVillageWiseModel>();

            while (drInner.Read())
            {
                var SList = new VMGetVillageWiseModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Village_Name = drInner["village_name"].ToString();
                SList.Village_Gid = drInner["village_gid"].ToString();
                SList.Total_Population = drInner["count"].ToString();

                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid, COUNT(*) \r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nwhere date_part('year',age(fmm.birth_date))>='18'\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid";
                cmdInner.CommandText = "SELECT * FROM public.getvillagewise1_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Population_Age = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid, COUNT(*) \r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid";
                cmdInner.CommandText = "SELECT * FROM public.getvillagewise2_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Total_Screening = drInner1["count"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM'), 0) * 100,0) AS diabetes_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getvillagewise3_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Confirmed_Diabetes_Mellitus = drInner1["confirmed_diabetes_mellitus"].ToString();

                            RList[i].Controlled_Diabetes_Mellitus = drInner1["controlled_diabetes_mellitus"].ToString();

                            RList[i].Diabetes_Percentage = drInner1["diabetes_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) AS hypertension_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getvillagewise4_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Confirmed_Hypertension = drInner1["confirmed_hypertension"].ToString();

                            RList[i].Controlled_Hypertension = drInner1["controlled_hypertension"].ToString();

                            RList[i].Hypertension_Percentage = drInner1["hypertension_percentage"].ToString();
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

                //cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0)\r\nAS both_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid\r\n";
                cmdInner.CommandText = "SELECT * FROM public.getvillagewise5_dph(' " + CommunityParam + " ')";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Confirmed_Both = drInner1["confirmed_both"].ToString();

                            RList[i].Controlled_Both = drInner1["controlled_both"].ToString();

                            RList[i].Both_Percentage = drInner1["both_percentage"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetVillageWiseModel[] RArray = RList.ToArray();

            return RArray;
        }
    }
}
