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

namespace PHRLockerAPI.Controllers
{
    //[Route("api/[controller]")]
    [ApiController]
    public class WebAPINewController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        private readonly DapperContext _context;
        string CommunityParam = "";
        string InstitutionParam = "";



        private readonly DapperContext context;

        public WebAPINewController(DapperContext context, IConfiguration configuration)

        {
            this.context = context;
            _configuration = configuration;
            _context = context;
        }

        //public WebAPINewController(IConfiguration configuration)
        //{
        //    _configuration = configuration;
        //}

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
            if (F.facility_id != "")
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
            if (F.indistrict_id != "")
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
            if (F.inhud_id != "")
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
            if (F.inblock_id != "")
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
            if (F.infacility_id != "")
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
            if (F.directorate_id != "")
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
            if (F.role != "")
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



        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getadmincount")]
        public VMadminapi GetAdminCounts(Adminapimodel A)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMadminapi VM = new VMadminapi();

            string Param = "";

            if (A.district_id != "" && A.block_id != "")
            {
                Param = "and district_id='" + A.district_id + "' and block_id='" + A.block_id + "'";
            }
            else if (A.district_id != "")
            {
                Param = "and district_id='" + A.district_id + "'";
            }
            else if (A.block_id != "")
            {
                Param = "and block_id='" + A.block_id + "'";
            }

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(street_id) hsc_map_count from address_street_master where facility_id is not null " + Param + " ";

            NpgsqlDataReader dr = cmd.ExecuteReader();

            while (dr.Read())
            {
                VM.hsc_map_count = dr["hsc_map_count"].ToString();
            }

            con.Close();

            con.Open();
            NpgsqlCommand cmdhsc_not_count = new NpgsqlCommand();
            cmdhsc_not_count.Connection = con;
            cmdhsc_not_count.CommandType = CommandType.Text;
            cmdhsc_not_count.CommandText = "select count(street_id) hsc_not_count from address_street_master where facility_id is null " + Param + " ";

            NpgsqlDataReader drhsc_not_count = cmdhsc_not_count.ExecuteReader();

            while (drhsc_not_count.Read())
            {
                VM.hsc_not_count = drhsc_not_count["hsc_not_count"].ToString();
            }

            con.Close();

            con.Open();
            NpgsqlCommand cmdrev_map_count = new NpgsqlCommand();
            cmdrev_map_count.Connection = con;
            cmdrev_map_count.CommandType = CommandType.Text;
            cmdrev_map_count.CommandText = "select count(street_id) rev_map_count from address_street_master where rev_village_id is not null " + Param + " ";

            NpgsqlDataReader drrev_map_count = cmdrev_map_count.ExecuteReader();

            while (drrev_map_count.Read())
            {
                VM.rev_map_count = drrev_map_count["rev_map_count"].ToString();
            }

            con.Close();

            con.Open();
            NpgsqlCommand cmdrev_not_count = new NpgsqlCommand();
            cmdrev_not_count.Connection = con;
            cmdrev_not_count.CommandType = CommandType.Text;
            cmdrev_not_count.CommandText = "select count(street_id) rev_not_count from address_street_master where rev_village_id is null " + Param + " ";

            NpgsqlDataReader drrev_not_count = cmdrev_not_count.ExecuteReader();

            while (drrev_not_count.Read())
            {
                VM.rev_not_count = drrev_not_count["rev_not_count"].ToString();
            }

            con.Close();

            con.Open();
            NpgsqlCommand cmdstreetMappedCount = new NpgsqlCommand();
            cmdstreetMappedCount.Connection = con;
            cmdstreetMappedCount.CommandType = CommandType.Text;
            cmdstreetMappedCount.CommandText = "select count(shop_id) streetMappedCount from address_shop_master where street_gid is not null " + Param + " ";

            NpgsqlDataReader drstreetMappedCount = cmdstreetMappedCount.ExecuteReader();

            while (drstreetMappedCount.Read())
            {
                VM.streetMappedCount = drstreetMappedCount["streetMappedCount"].ToString();
            }

            con.Close();


            con.Open();
            NpgsqlCommand cmdstreetnotMappedCount = new NpgsqlCommand();
            cmdstreetnotMappedCount.Connection = con;
            cmdstreetnotMappedCount.CommandType = CommandType.Text;
            cmdstreetnotMappedCount.CommandText = "select count(shop_id) streetnotMappedCount from address_shop_master where street_gid is null " + Param + " ";

            NpgsqlDataReader drstreetnotMappedCount = cmdstreetnotMappedCount.ExecuteReader();

            while (drstreetnotMappedCount.Read())
            {
                VM.streetnotMappedCount = drstreetnotMappedCount["streetnotMappedCount"].ToString();
            }

            con.Close();


            con.Open();
            NpgsqlCommand cmdVillage = new NpgsqlCommand();
            cmdVillage.Connection = con;
            cmdVillage.CommandType = CommandType.Text;
            cmdVillage.CommandText = "select count(village_id),village_type from address_village_master group by village_type order by count desc";

            NpgsqlDataReader drvillage = cmdVillage.ExecuteReader();


            List<VillageTypeWiseModel> VList = new List<VillageTypeWiseModel>();

            while (drvillage.Read())
            {
                VillageTypeWiseModel V = new VillageTypeWiseModel();

                V.village_type = drvillage["village_type"].ToString();
                V.count = drvillage["count"].ToString();
                VList.Add(V);
            }

            con.Close();


            return VM;
        }



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetList")]
        public VMCommunityTriage Get()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid from public.address_district_master as MS order by district_name";

            cmd.CommandText = "SELECT * from public.getlist_1()";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {

                var SList = new CommunityTriageModel();

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
                //cmdInner.CommandText = "select SUM (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) as RiskScore,screening_values ->>'dm_risk_score' as ScreeningValues, MS.district_name,MS.district_gid from public.health_screening as S inner join public.family_member_master as M on M.member_id=S.member_id inner join public.address_district_master as MS on MS.district_id=M.district_id where screening_values ->>'dm_risk_score' is not null  and (CAST (screening_values ->>'dm_risk_score' AS INTEGER))!=0 and (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) not in (5,6) group by screening_values ->>'dm_risk_score',MS.district_name,MS.district_gid order by MS.district_name limit 100";

                cmdInner.CommandText = "SELECT * from public.getlist_2()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                CommunityTriageModel SList = new CommunityTriageModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;

                        if (drInner["ScreeningValues"].ToString() == "1" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].NormalRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "2" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].MediumRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "3" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].LowRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "4" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].HighRisk = drInner["RiskScore"].ToString();
                        }
                    }
                }

            }


            con.Close();

            /*Hud Wise*/

            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            //cmdHud.CommandText = "select hud_id,hud_name,hud_gid from public.address_hud_master order by hud_name";

            cmdHud.CommandText = "SELECT * from public.getlist_3()";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<HudModel> RListHud = new List<HudModel>();

            while (drHud.Read())
            {

                var SList = new HudModel();

                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                SList.hud_id = drHud["hud_id"].ToString();

                RListHud.Add(SList);
            }


            con.Close();


            con.Open();

            if (RListHud.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "(select SUM (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) as RiskScore,screening_values ->>'dm_risk_score' as ScreeningValues, \r\nMS.hud_name,MS.hud_gid from public.health_screening as S\r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_hud_master as MS on MS.hud_id = M.hud_id\r\nwhere screening_values ->>'dm_risk_score' is not null  and (CAST (screening_values ->>'dm_risk_score' AS INTEGER))!=0\r\nand (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) not in (5,6)\r\ngroup by screening_values ->>'dm_risk_score',MS.hud_name,MS.hud_gid\r\norder by MS.hud_name)";

                cmdInner.CommandText = "SELECT * from public.getlist_4()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                HudModel SList = new HudModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        SList.hud_name = RListHud[i].hud_name;
                        SList.hud_gid = RListHud[i].hud_gid;

                        if (drInner["ScreeningValues"].ToString() == "1" & SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].NormalRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "2" & SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].MediumRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "3" & SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].LowRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "4" & SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].HighRisk = drInner["RiskScore"].ToString();
                        }
                    }
                }

            }

            con.Close();

            /*Block Wise*/

            con.Open();
            NpgsqlCommand cmdBlock = new NpgsqlCommand();
            cmdBlock.Connection = con;
            cmdBlock.CommandType = CommandType.Text;
            //cmdBlock.CommandText = "select block_id,block_name,block_gid from public.address_block_master order by block_name";

            cmdBlock.CommandText = "SELECT * from public.getlist_5()";

            NpgsqlDataReader drBlock = cmdBlock.ExecuteReader();
            List<BlockModel> RListBlock = new List<BlockModel>();

            while (drBlock.Read())
            {

                var SList = new BlockModel();

                SList.block_name = drBlock["block_name"].ToString();
                SList.block_gid = drBlock["block_gid"].ToString();
                SList.block_id = drBlock["block_id"].ToString();

                RListBlock.Add(SList);
            }


            con.Close();


            con.Open();

            if (RListBlock.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "(select SUM (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) as RiskScore,screening_values ->>'dm_risk_score' as ScreeningValues, \r\nMS.block_name,MS.block_gid from public.health_screening as S\r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_block_master as MS on MS.block_id = M.block_id\r\nwhere screening_values ->>'dm_risk_score' is not null  and (CAST (screening_values ->>'dm_risk_score' AS INTEGER))!=0\r\nand (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) not in (5,6)\r\ngroup by screening_values ->>'dm_risk_score',MS.block_name,MS.block_gid\r\norder by MS.block_name)";

                cmdInner.CommandText = "SELECT * from public.getlist_6()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                BlockModel SList = new BlockModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListBlock.Count; i++)
                    {
                        SList.block_name = RListBlock[i].block_name;
                        SList.block_gid = RListBlock[i].block_gid;

                        if (drInner["ScreeningValues"].ToString() == "1" & SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RListBlock[i].NormalRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "2" & SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RListBlock[i].MediumRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "3" & SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RListBlock[i].LowRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "4" & SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RListBlock[i].HighRisk = drInner["RiskScore"].ToString();
                        }
                    }
                }

            }

            con.Close();

            /*Village Wise*/

            con.Open();
            NpgsqlCommand cmdVillage = new NpgsqlCommand();
            cmdVillage.Connection = con;
            cmdVillage.CommandType = CommandType.Text;
            //cmdVillage.CommandText = "select village_id,village_name,village_gid from public.address_village_master order by village_name limit 10";

            cmdVillage.CommandText = "SELECT * from public.getlist_7()";

            NpgsqlDataReader drVillage = cmdVillage.ExecuteReader();
            List<VillageModel> RListVillage = new List<VillageModel>();

            while (drVillage.Read())
            {

                var SList = new VillageModel();

                SList.village_name = drVillage["village_name"].ToString();
                SList.village_gid = drVillage["village_gid"].ToString();
                SList.village_id = drVillage["village_id"].ToString();

                RListVillage.Add(SList);
            }


            con.Close();


            con.Open();

            if (RListVillage.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "(select SUM (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) as RiskScore,screening_values ->>'dm_risk_score' as ScreeningValues, \r\nMS.village_name,MS.village_gid from public.health_screening as S\r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_village_master as MS on MS.village_id = M.village_id\r\nwhere screening_values ->>'dm_risk_score' is not null  and (CAST (screening_values ->>'dm_risk_score' AS INTEGER))!=0\r\nand (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) not in (5,6)\r\ngroup by screening_values ->>'dm_risk_score',MS.village_name,MS.village_gid\r\norder by MS.village_name limit 50)";

                cmdInner.CommandText = "SELECT * from public.getlist_8()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VillageModel SList = new VillageModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListVillage.Count; i++)
                    {
                        SList.village_name = RListVillage[i].village_name;
                        SList.village_gid = RListVillage[i].village_gid;

                        if (drInner["ScreeningValues"].ToString() == "1" & SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RListVillage[i].NormalRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "2" & SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RListVillage[i].MediumRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "3" & SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RListVillage[i].LowRisk = drInner["RiskScore"].ToString();
                        }
                        else if (drInner["ScreeningValues"].ToString() == "4" & SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RListVillage[i].HighRisk = drInner["RiskScore"].ToString();
                        }
                    }
                }

            }


            con.Close();

            /*OverAll*/

            con.Open();
            NpgsqlCommand cmdAll = new NpgsqlCommand();
            cmdAll.Connection = con;
            cmdAll.CommandType = CommandType.Text;
            //cmdAll.CommandText = "(select SUM (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) as RiskScore,screening_values ->>'dm_risk_score' as ScreeningValues from public.health_screening as S\r\nwhere screening_values ->>'dm_risk_score' is not null  \r\nand (CAST (screening_values ->>'dm_risk_score' AS INTEGER))!=0\r\nand (CAST (screening_values ->>'dm_risk_score' AS INTEGER)) not in (5,6,7,8)\r\ngroup by screening_values ->>'dm_risk_score')";

            cmdAll.CommandText = "SELECT * from public.getlist_9()";

            NpgsqlDataReader drAll = cmdAll.ExecuteReader();

            while (drAll.Read())
            {

                var SList = new RiskScoreAllModel();

                SList.RiskScore = drAll["RiskScore"].ToString();
                SList.ScreeningValues = drAll["ScreeningValues"].ToString();

                if (SList.ScreeningValues == "1")
                {
                    VM.Normal = SList.RiskScore;
                }
                else if (SList.ScreeningValues == "2")
                {
                    VM.Medium = SList.RiskScore;
                }
                else if (SList.ScreeningValues == "3")
                {
                    VM.Low = SList.RiskScore;
                }
                else if (SList.ScreeningValues == "4")
                {
                    VM.High = SList.RiskScore;
                }
            }


            con.Close();


            VM.DistrictWise = RList;

            VM.HudWise = RListHud;

            VM.BlockWise = RListBlock;

            VM.VillageWise = RListVillage;

            //con.Close();
            return VM;
        }


        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart")]
        public VMMtmPerformance GetChartDetails(FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMMtmPerformance VM = new VMMtmPerformance();


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select count(S.screening_id)as Count,Gender from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ngroup by Gender limit 3";

            cmd.CommandText = "select count(screening_id)as Count,tbl.Gender from (SELECT B.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,screening_id, gender  FROM PUBLIC.health_screening B inner join family_member_master fm on B.member_id = fm.member_id   " + CommunityParam + " GROUP BY ARRUSER, screening_id, gender) tbl INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.Gender order by count desc limit 3";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<GenderWise> RList = new List<GenderWise>();

            while (dr.Read())
            {

                var SList = new GenderWise();

                SList.Gender = dr["Gender"].ToString();
                SList.Count = dr["Count"].ToString();

                RList.Add(SList);
            }


            con.Close();

            con.Open();
            NpgsqlCommand cmdAge = new NpgsqlCommand();
            cmdAge.Connection = con;
            cmdAge.CommandType = CommandType.Text;
            //cmdAge.CommandText = "SELECT \r\n       CASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END Age,count(M.family_id)\r\nFROM public.family_member_master as M \r\ninner join public.health_screening ON health_screening.member_id = M.member_id\r\ngroup by \r\nCASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END limit 3";

            cmdAge.CommandText = "select tblFinal.Age,sum(tblFinal.totalcount) count from(SELECT\r\nS.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,CASE \r\nWHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\nWHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\nWHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\nEND Age,count(fm.member_id ) totalcount\r\nFROM public.family_member_master as fm \r\ninner join public.health_screening S ON S.member_id = fm.member_id \r\n " + CommunityParam + " \r\ngroup by ARRUSER\r\n,Age)tblFinal\r\nINNER JOIN USER_MASTER UM ON CAST(tblFinal.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by tblFinal.Age\r\norder by count desc limit 3";

            NpgsqlDataReader drAge = cmdAge.ExecuteReader();
            List<AgeWiseModel> RListAge = new List<AgeWiseModel>();

            while (drAge.Read())
            {

                var SList = new AgeWiseModel();

                if (drAge["age"].ToString() == "18 to 30")
                {
                    VM.Middle = drAge["count"].ToString();

                }
                else if (drAge["age"].ToString() == "Below 18")
                {
                    VM.Below = drAge["count"].ToString();

                }
                else if (drAge["age"].ToString() == "Above 30")
                {
                    VM.Above = drAge["count"].ToString();
                }



                RListAge.Add(SList);
            }


            con.Close();

            con.Open();
            NpgsqlCommand cmdWeek = new NpgsqlCommand();
            cmdWeek.Connection = con;
            cmdWeek.CommandType = CommandType.Text;
            //cmdWeek.CommandText = "select to_char(Weekly, 'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS  weekly,COUNT(screening_id) AS count\r\nFROM public.health_screening GROUP BY weekly order by weekly desc limit 14)tbl";

            cmdWeek.CommandText = "select to_char(Weekly, 'mon') as Months,Count \r\nfrom(SELECT DATE_TRUNC('week',tbl.last_update_date)AS  weekly,COUNT(screening_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,screening_id,last_update_date  FROM PUBLIC.health_screening B\r\nWHERE JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,screening_id) tbl\r\ninner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl1";

            NpgsqlDataReader drWeek = cmdWeek.ExecuteReader();
            List<WeekModel> RListWeek = new List<WeekModel>();

            while (drWeek.Read())
            {

                var SList = new WeekModel();

                SList.Months = drWeek["Months"].ToString();
                SList.Count = drWeek["Count"].ToString();

                RListWeek.Add(SList);
            }


            con.Close();

            con.Open();
            NpgsqlCommand cmdBlock = new NpgsqlCommand();
            cmdBlock.Connection = con;
            cmdBlock.CommandType = CommandType.Text;
            //cmdBlock.CommandText = "select count(S.Screening_id)as Count,B.block_name from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_block_master as B on B.block_id = M.block_id\r\ngroup by B.block_name order by Count desc limit 5";

            cmdBlock.CommandText = "select sum(Scount)as Count,BL.block_name from (SELECT(B.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER,fm.family_id, count(screening_id) Scount, block_id FROM PUBLIC.health_screening B inner join family_master fm on b.family_id = fm.family_id  " + CommunityParam + " GROUP BY ARRUSER, screening_id, block_id, fm.family_id)tbl INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   " + InstitutionParam + "inner join public.address_block_master as BL on BL.block_id = tbl.block_id group by BL.block_name order by Count desc limit 5";

            NpgsqlDataReader drBlock = cmdBlock.ExecuteReader();
            List<BlockWiseModel> RListBlock = new List<BlockWiseModel>();

            while (drBlock.Read())
            {

                var SList = new BlockWiseModel();

                SList.Block_Name = drBlock["Block_Name"].ToString();
                SList.Count = drBlock["Count"].ToString();

                RListBlock.Add(SList);
            }


            con.Close();


            con.Open();

            NpgsqlCommand cmdWeekMTM = new NpgsqlCommand();
            cmdWeekMTM.Connection = con;
            cmdWeekMTM.CommandType = CommandType.Text;
            //cmdWeekMTM.CommandText = "select to_char(Weekly,'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS weekly,COUNT(medical_history_id) AS count\r\nFROM public.health_history GROUP BY weekly order by weekly desc limit 14)tbl";

            cmdWeekMTM.CommandText = "select to_char(Weekly,'mon') as Months,Count from\r\n(SELECT DATE_TRUNC('week',tbl1.last_update_date)AS weekly,COUNT(medical_history_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,last_update_date,medical_history_id  FROM PUBLIC.health_history B\r\nWHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' \r\nand JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,last_update_date,medical_history_id)tbl1\r\ninner join family_master fm on tbl1.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(tbl1.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl";

            NpgsqlDataReader drWeekMTM = cmdWeekMTM.ExecuteReader();
            List<WeekModel> RListWeekMTM = new List<WeekModel>();

            while (drWeekMTM.Read())
            {

                var SList = new WeekModel();

                SList.Months = drWeekMTM["Months"].ToString();
                SList.Count = drWeekMTM["Count"].ToString();

                RListWeekMTM.Add(SList);
            }

            con.Close();


            con.Open();
            NpgsqlCommand cmdBlockMTM = new NpgsqlCommand();
            cmdBlockMTM.Connection = con;
            cmdBlockMTM.CommandType = CommandType.Text;
            //cmdBlockMTM.CommandText = "select count(S.medical_history_id)as Count,B.block_name from public.health_history as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_block_master as B on B.block_id = M.block_id\r\nwhere S.mtm_beneficiary is not null\r\ngroup by B.block_name order by Count desc limit 10";

            //cmdBlockMTM.CommandText = "select count(medical_history_id)as Count,BL.block_name from \r\n(SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,medical_history_id  FROM PUBLIC.health_history B\r\nWHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' \r\nand JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,medical_history_id)tbl \r\ninner join family_master fm on tbl.family_id=fm.family_id   " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\ninner join public.address_block_master as BL on BL.block_id = fm.block_id\r\ngroup by BL.block_name order by Count desc limit 10";

            cmdBlockMTM.CommandText = "select sum(ScreeningCOunt)as Count,BL.block_name from (SELECT B.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,block_id, count(medical_history_id) ScreeningCOunt  FROM PUBLIC.health_history B inner join family_master fm on b.family_id = fm.family_id  " + CommunityParam + " WHERE CAST(mtm_beneficiary->> 'avail_service' as text) = 'yes' GROUP BY ARRUSER, block_id)tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "inner join public.address_block_master as BL on BL.block_id = tbl.block_id group by BL.block_name order by Count desc limit 10";

            NpgsqlDataReader drBlockMTM = cmdBlockMTM.ExecuteReader();
            List<BlockWiseModel> RListBlockMTM = new List<BlockWiseModel>();

            while (drBlockMTM.Read())
            {

                var SList = new BlockWiseModel();

                SList.Block_Name = drBlockMTM["Block_Name"].ToString();
                SList.Count = drBlockMTM["Count"].ToString();

                RListBlockMTM.Add(SList);
            }


            con.Close();

            con.Open();

            NpgsqlCommand cmdWeekDrug = new NpgsqlCommand();
            cmdWeekDrug.Connection = con;
            cmdWeekDrug.CommandType = CommandType.Text;
            //cmdWeekDrug.CommandText = "select to_char(Weekly,'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS weekly,COUNT(screening_id) AS count\r\nFROM public.health_screening where drugs !='null' GROUP BY weekly order by weekly desc limit 14)tbl";

            cmdWeekDrug.CommandText = "select to_char(Weekly,'mon') as Months,Count from\r\n(SELECT DATE_TRUNC('week',tbl.last_update_date)AS weekly,COUNT(screening_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nscreening_id,family_id,last_update_date  FROM PUBLIC.HEALTH_SCREENING B\r\nWHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' \r\nGROUP BY ARRUSER,screening_id,last_update_date) tbl\r\ninner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl1";

            NpgsqlDataReader drWeekMTMDrug = cmdWeekDrug.ExecuteReader();
            List<WeekModel> RListWeekMTMDrug = new List<WeekModel>();

            while (drWeekMTMDrug.Read())
            {

                var SList = new WeekModel();

                SList.Months = drWeekMTMDrug["Months"].ToString();
                SList.Count = drWeekMTMDrug["Count"].ToString();

                RListWeekMTMDrug.Add(SList);
            }

            con.Close();

            con.Open();

            NpgsqlCommand cmdDrug = new NpgsqlCommand();
            cmdDrug.Connection = con;
            cmdDrug.CommandType = CommandType.Text;
            //cmdDrug.CommandText = "select Drugarray->>'drug_name'as drug,count(screening_id)as Count from(SELECT jsonb_array_elements(b.drugs) AS Drugarray,screening_id\r\nFROM   public.health_screening b WHERE  jsonb_typeof(b.drugs) = 'array')tbl group by drug order by Count desc limit 10";

            cmdDrug.CommandText = "select Drugarray->>'drug_name'as drug,count(screening_id)as Count from\r\n(SELECT jsonb_array_elements(b.drugs) AS Drugarray,JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nscreening_id,family_id  FROM PUBLIC.HEALTH_SCREENING B\r\nWHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' \r\nGROUP BY ARRUSER,screening_id,Drugarray)tbl \r\ninner join family_master fm on tbl.family_id=fm.family_id   " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "   \r\ngroup by drug order by Count desc limit 10";

            NpgsqlDataReader drDrug = cmdDrug.ExecuteReader();
            List<DrugModel> RListDrug = new List<DrugModel>();

            while (drDrug.Read())
            {

                var SList = new DrugModel();

                SList.drug = drDrug["drug"].ToString();
                SList.count = drDrug["Count"].ToString();

                RListDrug.Add(SList);
            }

            con.Close();




            VM.GenderList = RList;
            VM.AgeList = RListAge;
            VM.WeekList = RListWeek;
            VM.BlockList = RListBlock;
            VM.MTMWeekList = RListWeekMTM;
            VM.MTMBlockList = RListBlockMTM;
            VM.MTMWeekListDrug = RListWeekMTMDrug;
            VM.DrugList = RListDrug;

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart1")]


        public async Task<IEnumerable<GenderWise>> GetChartDetails1([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart1('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<GenderWise>(query);
                return OBJ.ToList();
            }
        }

        //public List<GenderWise> GetChartDetails1([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    //cmd.CommandText = "select count(S.screening_id)as Count,Gender from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ngroup by Gender limit 3";

        //    cmd.CommandText = "select count(screening_id)as Count,tbl.Gender from (SELECT B.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,screening_id, gender  FROM PUBLIC.health_screening B inner join family_member_master fm on B.member_id = fm.member_id   " + CommunityParam + " GROUP BY ARRUSER, screening_id, gender) tbl INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.Gender order by count desc limit 3";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();
        //    List<GenderWise> RList = new List<GenderWise>();

        //    while (dr.Read())
        //    {

        //        var SList = new GenderWise();

        //        SList.Gender = dr["Gender"].ToString();
        //        SList.Count = dr["Count"].ToString();

        //        RList.Add(SList);
        //    }


        //    con.Close();

        //    VM.GenderList = RList;


        //    return RList;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart2")]
        public async Task<IEnumerable<AgeWiseDisplayModel>> GetChartDetails2([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart2('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<AgeWiseDisplayModel>(query);
                return OBJ.ToList();
            }
        }

        //public AgeWiseDisplayModel GetChartDetails2([FromQuery] FilterpayloadModel F)
        //{

        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    AgeWiseDisplayModel VM = new AgeWiseDisplayModel();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmdAge = new NpgsqlCommand();
        //    cmdAge.Connection = con;
        //    cmdAge.CommandType = CommandType.Text;
        //    //cmdAge.CommandText = "SELECT \r\n       CASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END Age,count(M.family_id)\r\nFROM public.family_member_master as M \r\ninner join public.health_screening ON health_screening.member_id = M.member_id\r\ngroup by \r\nCASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END limit 3";

        //    cmdAge.CommandText = "select tblFinal.Age,sum(tblFinal.totalcount) count from(SELECT\r\nS.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,CASE \r\nWHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\nWHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\nWHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\nEND Age,count(fm.member_id ) totalcount\r\nFROM public.family_member_master as fm \r\ninner join public.health_screening S ON S.member_id = fm.member_id \r\n " + CommunityParam + " \r\ngroup by ARRUSER\r\n,Age)tblFinal\r\nINNER JOIN USER_MASTER UM ON CAST(tblFinal.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by tblFinal.Age\r\norder by count desc limit 3";

        //    NpgsqlDataReader drAge = cmdAge.ExecuteReader();
        //    List<AgeWiseDisplayModel> RListAge = new List<AgeWiseDisplayModel>();

        //    while (drAge.Read())
        //    {

        //        var SList = new AgeWiseDisplayModel();

        //        if (drAge["age"].ToString() == "18 to 30")
        //        {
        //            VM.middle = drAge["count"].ToString();

        //        }
        //        else if (drAge["age"].ToString() == "Below 18")
        //        {
        //            VM.below = drAge["count"].ToString();

        //        }
        //        else if (drAge["age"].ToString() == "Above 30")
        //        {
        //            VM.above = drAge["count"].ToString();
        //        }



        //        RListAge.Add(SList);
        //    }

        //    con.Close();
        //    //VM.AgeList = RListAge;


        //    return VM;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart3")]

        public async Task<IEnumerable<WeekModel>> GetChartDetails3([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart3('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<WeekModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<WeekModel> GetChartDetails3([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmdWeek = new NpgsqlCommand();
        //    cmdWeek.Connection = con;
        //    cmdWeek.CommandType = CommandType.Text;
        //    //cmdWeek.CommandText = "select to_char(Weekly, 'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS  weekly,COUNT(screening_id) AS count\r\nFROM public.health_screening GROUP BY weekly order by weekly desc limit 14)tbl";

        //    cmdWeek.CommandText = "select to_char(Weekly, 'mon') as Months,Count \r\nfrom(SELECT DATE_TRUNC('week',tbl.last_update_date)AS  weekly,COUNT(screening_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,screening_id,last_update_date  FROM PUBLIC.health_screening B\r\nWHERE JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,screening_id) tbl\r\ninner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl1";

        //    NpgsqlDataReader drWeek = cmdWeek.ExecuteReader();
        //    List<WeekModel> RListWeek = new List<WeekModel>();

        //    while (drWeek.Read())
        //    {

        //        var SList = new WeekModel();

        //        SList.Months = drWeek["Months"].ToString();
        //        SList.Count = drWeek["Count"].ToString();

        //        RListWeek.Add(SList);
        //    }


        //    con.Close();


        //    VM.WeekList = RListWeek;


        //    return RListWeek;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart4")]

        public async Task<IEnumerable<AgeWiseDisplayModel>> GetChartDetails4([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart4('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<AgeWiseDisplayModel>(query);
                return OBJ.ToList();
            }
        }

        //public VMMtmPerformance GetChartDetails4([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmdAge = new NpgsqlCommand();
        //    cmdAge.Connection = con;
        //    cmdAge.CommandType = CommandType.Text;
        //    //cmdAge.CommandText = "SELECT \r\n       CASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END Age,count(M.family_id)\r\nFROM public.family_member_master as M \r\ninner join public.health_screening ON health_screening.member_id = M.member_id\r\ngroup by \r\nCASE \r\n           WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\n           WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\n           WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\n       END limit 3";

        //    cmdAge.CommandText = "select tblFinal.Age,sum(tblFinal.totalcount) count from(SELECT\r\nS.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,CASE \r\nWHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'\r\nWHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'\r\nWHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'\r\nEND Age,count(fm.member_id ) totalcount\r\nFROM public.family_member_master as fm \r\ninner join public.health_screening S ON S.member_id = fm.member_id \r\n " + CommunityParam + " \r\ngroup by ARRUSER\r\n,Age)tblFinal\r\nINNER JOIN USER_MASTER UM ON CAST(tblFinal.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by tblFinal.Age\r\norder by count desc limit 3";

        //    NpgsqlDataReader drAge = cmdAge.ExecuteReader();
        //    List<AgeWiseModel> RListAge = new List<AgeWiseModel>();

        //    while (drAge.Read())
        //    {

        //        var SList = new AgeWiseModel();

        //        if (drAge["age"].ToString() == "18 to 30")
        //        {
        //            VM.Middle = drAge["count"].ToString();

        //        }
        //        else if (drAge["age"].ToString() == "Below 18")
        //        {
        //            VM.Below = drAge["count"].ToString();

        //        }
        //        else if (drAge["age"].ToString() == "Above 30")
        //        {
        //            VM.Above = drAge["count"].ToString();
        //        }



        //        RListAge.Add(SList);
        //    }


        //    con.Close();


        //    VM.AgeList = RListAge;


        //    return VM;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart5")]


        public async Task<IEnumerable<BlockWiseModel>> GetChartDetails5([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart5('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<BlockWiseModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<BlockWiseModel> GetChartDetails5([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmdBlock = new NpgsqlCommand();
        //    cmdBlock.Connection = con;
        //    cmdBlock.CommandType = CommandType.Text;
        //    //cmdBlock.CommandText = "select count(S.Screening_id)as Count,B.block_name from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_block_master as B on B.block_id = M.block_id\r\ngroup by B.block_name order by Count desc limit 5";

        //    cmdBlock.CommandText = "select sum(Scount)as Count,BL.block_name from (SELECT(B.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER,fm.family_id, count(screening_id) Scount, block_id FROM PUBLIC.health_screening B inner join family_master fm on b.family_id = fm.family_id  " + CommunityParam + " GROUP BY ARRUSER, screening_id, block_id, fm.family_id)tbl INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   " + InstitutionParam + "inner join public.address_block_master as BL on BL.block_id = tbl.block_id group by BL.block_name order by Count desc limit 5";

        //    NpgsqlDataReader drBlock = cmdBlock.ExecuteReader();
        //    List<BlockWiseModel> RListBlock = new List<BlockWiseModel>();

        //    while (drBlock.Read())
        //    {

        //        var SList = new BlockWiseModel();

        //        SList.Block_Name = drBlock["Block_Name"].ToString();
        //        SList.Count = drBlock["Count"].ToString();

        //        RListBlock.Add(SList);
        //    }


        //    con.Close();


        //    VM.BlockList = RListBlock;


        //    return RListBlock;

        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart6")]

        public async Task<IEnumerable<WeekModel>> GetChartDetails6([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart6('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<WeekModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<WeekModel> GetChartDetails6([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();

        //    NpgsqlCommand cmdWeekMTM = new NpgsqlCommand();
        //    cmdWeekMTM.Connection = con;
        //    cmdWeekMTM.CommandType = CommandType.Text;
        //    //cmdWeekMTM.CommandText = "select to_char(Weekly,'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS weekly,COUNT(medical_history_id) AS count\r\nFROM public.health_history GROUP BY weekly order by weekly desc limit 14)tbl";

        //    cmdWeekMTM.CommandText = "select to_char(Weekly,'mon') as Months,Count from\r\n(SELECT DATE_TRUNC('week',tbl1.last_update_date)AS weekly,COUNT(medical_history_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,last_update_date,medical_history_id  FROM PUBLIC.health_history B\r\nWHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' \r\nand JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,last_update_date,medical_history_id)tbl1\r\ninner join family_master fm on tbl1.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(tbl1.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl";

        //    NpgsqlDataReader drWeekMTM = cmdWeekMTM.ExecuteReader();
        //    List<WeekModel> RListWeekMTM = new List<WeekModel>();

        //    while (drWeekMTM.Read())
        //    {

        //        var SList = new WeekModel();

        //        SList.Months = drWeekMTM["Months"].ToString();
        //        SList.Count = drWeekMTM["Count"].ToString();

        //        RListWeekMTM.Add(SList);
        //    }

        //    con.Close();

        //    VM.MTMWeekList = RListWeekMTM;


        //    return RListWeekMTM;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart7")]


        public async Task<IEnumerable<BlockWiseModel>> GetChartDetails7([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart7('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<BlockWiseModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<BlockWiseModel> GetChartDetails7([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmdBlockMTM = new NpgsqlCommand();
        //    cmdBlockMTM.Connection = con;
        //    cmdBlockMTM.CommandType = CommandType.Text;
        //    //cmdBlockMTM.CommandText = "select count(S.medical_history_id)as Count,B.block_name from public.health_history as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_block_master as B on B.block_id = M.block_id\r\nwhere S.mtm_beneficiary is not null\r\ngroup by B.block_name order by Count desc limit 10";

        //    //cmdBlockMTM.CommandText = "select count(medical_history_id)as Count,BL.block_name from \r\n(SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,medical_history_id  FROM PUBLIC.health_history B\r\nWHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' \r\nand JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,medical_history_id)tbl \r\ninner join family_master fm on tbl.family_id=fm.family_id   " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " \r\ninner join public.address_block_master as BL on BL.block_id = fm.block_id\r\ngroup by BL.block_name order by Count desc limit 10";

        //    cmdBlockMTM.CommandText = "select sum(ScreeningCOunt)as Count,BL.block_name from (SELECT B.UPDATE_REGISTER->0->> 'user_id' AS ARRUSER,block_id, count(medical_history_id) ScreeningCOunt  FROM PUBLIC.health_history B inner join family_master fm on b.family_id = fm.family_id  " + CommunityParam + " WHERE CAST(mtm_beneficiary->> 'avail_service' as text) = 'yes' GROUP BY ARRUSER, block_id)tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "inner join public.address_block_master as BL on BL.block_id = tbl.block_id group by BL.block_name order by Count desc limit 10";

        //    NpgsqlDataReader drBlockMTM = cmdBlockMTM.ExecuteReader();
        //    List<BlockWiseModel> RListBlockMTM = new List<BlockWiseModel>();

        //    while (drBlockMTM.Read())
        //    {

        //        var SList = new BlockWiseModel();

        //        SList.Block_Name = drBlockMTM["Block_Name"].ToString();
        //        SList.Count = drBlockMTM["Count"].ToString();

        //        RListBlockMTM.Add(SList);
        //    }


        //    con.Close();

        //    VM.MTMBlockList = RListBlockMTM;


        //    return RListBlockMTM;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetChart8")]

        public async Task<IEnumerable<WeekModel>> GetChartDetails8([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart8('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<WeekModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<WeekModel> GetChartDetails8([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();

        //    NpgsqlCommand cmdWeekDrug = new NpgsqlCommand();
        //    cmdWeekDrug.Connection = con;
        //    cmdWeekDrug.CommandType = CommandType.Text;
        //    //cmdWeekDrug.CommandText = "select to_char(Weekly,'mon') as Months,Count from(SELECT DATE_TRUNC('week',last_update_date)AS weekly,COUNT(screening_id) AS count\r\nFROM public.health_screening where drugs !='null' GROUP BY weekly order by weekly desc limit 14)tbl";

        //    cmdWeekDrug.CommandText = "select to_char(Weekly,'mon') as Months,Count from\r\n(SELECT DATE_TRUNC('week',tbl.last_update_date)AS weekly,COUNT(screening_id) AS count\r\nFROM (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nscreening_id,family_id,last_update_date  FROM PUBLIC.HEALTH_SCREENING B\r\nWHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' \r\nGROUP BY ARRUSER,screening_id,last_update_date) tbl\r\ninner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   " + InstitutionParam + " \r\nGROUP BY weekly order by weekly desc limit 14)tbl1";

        //    NpgsqlDataReader drWeekMTMDrug = cmdWeekDrug.ExecuteReader();
        //    List<WeekModel> RListWeekMTMDrug = new List<WeekModel>();

        //    while (drWeekMTMDrug.Read())
        //    {

        //        var SList = new WeekModel();

        //        SList.Months = drWeekMTMDrug["Months"].ToString();
        //        SList.Count = drWeekMTMDrug["Count"].ToString();

        //        RListWeekMTMDrug.Add(SList);
        //    }

        //    con.Close();

        //    VM.MTMWeekListDrug = RListWeekMTMDrug;


        //    return RListWeekMTMDrug;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Authorize]
        [Route("GetChart9")]

        public async Task<IEnumerable<DrugModel>> GetChartDetails9([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getchart9('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DrugModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<DrugModel> GetChartDetails9([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
        //    VMMtmPerformance VM = new VMMtmPerformance();


        //    Filterforall(F);

        //    con.Open();

        //    NpgsqlCommand cmdDrug = new NpgsqlCommand();
        //    cmdDrug.Connection = con;
        //    cmdDrug.CommandType = CommandType.Text;
        //    //cmdDrug.CommandText = "select Drugarray->>'drug_name'as drug,count(screening_id)as Count from(SELECT jsonb_array_elements(b.drugs) AS Drugarray,screening_id\r\nFROM   public.health_screening b WHERE  jsonb_typeof(b.drugs) = 'array')tbl group by drug order by Count desc limit 10";

        //    cmdDrug.CommandText = "select Drugarray->>'drug_name'as drug,count(screening_id)as Count from\r\n(SELECT jsonb_array_elements(b.drugs) AS Drugarray,JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nscreening_id,family_id  FROM PUBLIC.HEALTH_SCREENING B\r\nWHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' \r\nGROUP BY ARRUSER,screening_id,Drugarray)tbl \r\ninner join family_master fm on tbl.family_id=fm.family_id   " + CommunityParam + " \r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "   \r\ngroup by drug order by Count desc limit 10";

        //    NpgsqlDataReader drDrug = cmdDrug.ExecuteReader();
        //    List<DrugModel> RListDrug = new List<DrugModel>();

        //    while (drDrug.Read())
        //    {

        //        var SList = new DrugModel();

        //        SList.drug = drDrug["drug"].ToString();
        //        SList.count = drDrug["Count"].ToString();

        //        RListDrug.Add(SList);
        //    }

        //    con.Close();

        //    VM.DrugList = RListDrug;


        //    return RListDrug;

        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Authorize]
        [Route("GetOPDashboard")]
        public List<VMOPDashboardFacility> GetOPdasdhboard()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMOPDashboardFacility VM = new VMOPDashboardFacility();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select fr.facility_name,fr.facility_id,fcm.category_name,ftm.facility_type_name,fdm.directorate_name,\r\nadm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,is_hwc\r\nfrom facility_registry fr\r\ninner join facility_category_master fcm on fr.category_id=fcm.Category_id\r\ninner join facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id\r\ninner join facility_directorate_master fdm on fr.directorate_id=fdm.directorate_id\r\ninner join address_district_master adm on adm.district_id=fr.district_id\r\ninner join address_hud_master ahm on ahm.hud_id=fr.hud_id\r\ninner join address_block_master abm on abm.block_id=fr.block_id where fr.facility_level='HSC' or facility_level='PHC' order by adm.district_name  limit 1000";

            cmd.CommandText = "SELECT * from public.getopdashboard()";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMOPDashboardFacility> RList = new List<VMOPDashboardFacility>();

            while (dr.Read())
            {

                var SList = new VMOPDashboardFacility();

                SList.facility_id = dr["facility_id"].ToString();
                SList.facility_name = dr["facility_name"].ToString();
                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.directorate_name = dr["directorate_name"].ToString();
                SList.block_name = dr["block_name"].ToString();
                SList.block_gid = dr["block_gid"].ToString();
                SList.facility_type_name = dr["facility_type_name"].ToString();
                SList.is_hwc = dr["is_hwc"].ToString();

                RList.Add(SList);
            }

            con.Close();


            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select facility_id,CASE \r\nWHEN age between 0 and 17 THEN 'child' \r\nWHEN age >18 THEN 'Adult'  \r\nEND age2,tbl.gender,sum(sccoun) totc from \r\n    (SELECT jsonb_array_elements(b.update_register)->>'user_id' AS Drugarray,\r\n     date_part('year',age(birth_date)) age,gender,count(screening_id) sccoun\r\nFROM   public.health_screening b \r\n    inner join family_member_master fm on b.member_id=fm.member_id\r\n    group by Drugarray,gender,age) tbl\r\n    inner join user_master um on tbl.Drugarray=cast(um.user_id as text)\r\n    group by um.facility_id,age2,tbl.gender";

                cmdInner.CommandText = "SELECT * from public.getopdashboard_2()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].facility_id == drInner["facility_id"].ToString())
                        {
                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Male")
                            {
                                RList[i].AdultOPMale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Female")
                            {
                                RList[i].AdultOPFemale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "Adult" & drInner["gender"].ToString() == "Transgender")
                            {
                                RList[i].AdultOPtransgender = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            RList[i].AdultTotal = (RList[i].AdultOPMale) + (RList[i].AdultOPFemale) + (RList[i].AdultOPtransgender);


                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Male")
                            {
                                RList[i].ChildrenOPMale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Female")
                            {
                                RList[i].ChildrenOPFemale = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            if (drInner["age2"].ToString() == "child" & drInner["gender"].ToString() == "Transgender")
                            {
                                RList[i].ChildrenOPtransgender = Convert.ToInt32(drInner["totc"].ToString());
                            }

                            RList[i].ChildrenTotal = (RList[i].ChildrenOPMale) + (RList[i].ChildrenOPFemale) + (RList[i].ChildrenOPtransgender);

                            RList[i].TotalOPMale = RList[i].AdultOPMale + RList[i].ChildrenOPMale;
                            RList[i].TotalOPFemale = RList[i].AdultOPFemale + RList[i].ChildrenOPFemale;
                            RList[i].TotalOPtransgender = RList[i].AdultOPtransgender + RList[i].ChildrenOPtransgender;

                            RList[i].TotalOP = RList[i].TotalOPMale + RList[i].TotalOPFemale + RList[i].TotalOPtransgender;

                        }

                    }
                }

            }


            con.Close();


            return RList;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFacilityScreeningFilter")]


        public async Task<IEnumerable<FacilityModel>> GetFacilityWiseScreeningFilter([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getfacilityscreeningfilter('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<FacilityModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<FacilityModel> GetFacilityWiseScreeningFilter([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();

        //    Filterforall(F);

        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "SELECT Fr.facility_name,count(screening_id) count\r\nFROM   public.health_screening b \r\ninner join family_member_master fm on b.member_id=fm.member_id  " + CommunityParam + "\r\ninner join facility_registry Fr on Fr.facility_id = fm.facility_id  " + InstitutionParam + "\r\ngroup by Fr.facility_name order by count desc limit 10";

        //    List<FacilityModel> Flist = new List<FacilityModel>();

        //    NpgsqlDataReader dr = cmd.ExecuteReader();


        //    while (dr.Read())
        //    {

        //        var SList = new FacilityModel();


        //        SList.facility_name = dr["facility_name"].ToString();
        //        SList.count = dr["count"].ToString();

        //        Flist.Add(SList);
        //    }

        //    con.Close();



        //    return Flist;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]

        [Route("GetFacilityScreening")]
        public List<FacilitynamecountModel> GetFacilityWiseScreening()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "select * from public.getfacilityscreening()";



            //cmd.CommandText = "SELECT Fr.facility_name,count(screening_id) count\r\nFROM   public.health_screening b \r\ninner join family_member_master fm on b.member_id=fm.member_id\r\ninner join facility_registry Fr on Fr.facility_id = fm.facility_id\r\ngroup by Fr.facility_name order by count desc limit 10";

            List<FacilitynamecountModel> Flist = new List<FacilitynamecountModel>();

            NpgsqlDataReader dr = cmd.ExecuteReader();


            while (dr.Read())
            {

                var SList = new FacilitynamecountModel();


                SList.facility_name = dr["facility_name"].ToString();
                SList.count = dr["count"].ToString();

                Flist.Add(SList);
            }

            con.Close();



            return Flist;
        }



        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getdistrictpbs")]

        public async Task<Getdistrictpbs> districtpbs(FilterpayloadModel F)
        {
            Getdistrictpbs VM = new Getdistrictpbs();
            Filterforall(F);
            var parameters = new { CommunityParam = CommunityParam };

            string query = "select * from public.Getdistrictpbs(@CommunityParam)";
            List<Getdistrictpbs> RList = new List<Getdistrictpbs>();

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<Getdistrictpbs>(query, parameters);
                foreach (var result in results)
                {
                    result.screeningCount = "0";
                    result.uniqueCount = "0";
                    RList.Add(result);
                }
            }
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n group by fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n inner join address_district_master dm on tbl.district_id=dm.district_id \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getdistrictpbs SList = new Getdistrictpbs();
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

                cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getdistrictpbs SList = new Getdistrictpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
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

                cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from (select fm.district_id, district_name, district_gid, member_id, count(screening_id), JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_district_master dm on fm.district_id = dm.district_id where (hh.diseases->0->> 'outcome' = 'Referred out' or hh.diseases->0->> 'outcome' = 'Referred Out')group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.district_id,district_name,district_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getdistrictpbs SList = new Getdistrictpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.district_name = RList[i].district_name;
                        SList.district_gid = RList[i].district_gid;
                        if (SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].referredscreening = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();

            VM.DistrictWise = RList;

            return VM;
        }


        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Gethudpbs")]
        public async Task<Gethudpbs> hudpbs(FilterpayloadModel F)
        {

            Filterforall(F);

            var parameters = new { CommunityParam = CommunityParam };
            string query = "select * from public.Gethudpbs(@CommunityParam)";
            Gethudpbs VM = new Gethudpbs();
            List<Gethudpbs> RList = new List<Gethudpbs>();

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<Gethudpbs>(query, parameters);
                foreach (var result in results)
                {
                    result.screeningCount = "0";
                    result.uniqueCount = "0";
                    RList.Add(result);
                }
            }
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.hud_id,tbl.hud_name,tbl.hud_gid,count(member_id) TotalCount from (select fm.hud_id, H.hud_name,H.hud_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_hud_master H on H.hud_id = fm.hud_id group by fm.hud_id, H.hud_name, H.hud_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.hud_id,tbl.hud_name,tbl.hud_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Gethudpbs SList = new Gethudpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.hud_name = RList[i].hud_name;
                        SList.hud_id = RList[i].hud_id;
                        SList.hud_gid = RList[i].hud_gid;
                        if (SList.hud_gid == drInner["hud_gid"].ToString())
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

                cmdInner.CommandText = " select tbl.hud_id,hud_name,hud_gid,count(member_id) TotalCount from (select fm.hud_id, hud_name, hud_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_hud_master H on H.hud_id = fm.hud_id group by fm.hud_id, hud_name, hud_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "group by tbl.hud_id,hud_name,hud_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Gethudpbs SList = new Gethudpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.hud_name = RList[i].hud_name;
                        SList.hud_gid = RList[i].hud_gid;
                        if (SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
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

                cmdInner.CommandText = "select tbl.hud_id,hud_name,hud_gid,count(member_id) TotalCount from (select fm.hud_id, hud_name, hud_gid, member_id, count(screening_id), JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner                                                     join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner                                              join address_hud_master hd on fm.hud_id = hd.hud_id where (hh.diseases->0->> 'outcome' = 'Referred out' or hh.diseases->0->> 'outcome' = 'Referred Out')group by fm.hud_id,hud_name,hud_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.hud_id,hud_name,hud_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Gethudpbs SList = new Gethudpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.hud_name = RList[i].hud_name;
                        SList.hud_gid = RList[i].hud_gid;
                        if (SList.hud_gid == drInner["hud_gid"].ToString())
                        {
                            RList[i].referredscreening = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            VM.DistrictWise = RList;
            return VM;
        }

        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getblockpbs")]
        public async Task<Getblockpbs> blockpbs(FilterpayloadModel F)
        {
            Filterforall(F);

            var parameters = new { CommunityParam = CommunityParam };

            string query = "select * from public.Getblockpbs(@CommunityParam)";
            Getblockpbs VM = new Getblockpbs();
            List<Getblockpbs> RList = new List<Getblockpbs>();

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<Getblockpbs>(query, parameters);
                foreach (var result in results)
                {
                    result.screeningCount = "0";
                    result.uniqueCount = "0";
                    RList.Add(result);
                }
            }
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.block_id,block_name,block_gid,count(member_id) TotalCount from (select fm.block_id, block_name, block_gid, member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_block_master H on H.block_id = fm.block_id group by fm.block_id, block_name, block_gid,member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.block_id,block_name,block_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getblockpbs SList = new Getblockpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.block_name = RList[i].block_name;
                        SList.block_gid = RList[i].block_gid;
                        if (SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
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

                cmdInner.CommandText = "select tbl.block_id,block_name,block_gid,count(member_id) TotalCount from (select fm.block_id, block_name, block_gid, member_id, count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_block_master hd on fm.block_id = hd.block_id where(hh.diseases->0->> 'outcome' = 'Referred out' or hh.diseases->0->> 'outcome' = 'Referred Out') group by fm.block_id, block_name, block_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id')tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.block_id,block_name,block_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getblockpbs SList = new Getblockpbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.block_name = RList[i].block_name;
                        SList.block_gid = RList[i].block_gid;
                        if (SList.block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].referredscreening = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            VM.DistrictWise = RList;
            return VM;
        }


        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getvillagepbs")]
        public async Task<Getvillagepbs> villagepbs(FilterpayloadModel F)
        {
            Filterforall(F);
            var parameters = new { CommunityParam = CommunityParam };

            string query = "select * from public.getGetvillagepbs(@CommunityParam)";
            Getvillagepbs VM = new Getvillagepbs();
            List<Getvillagepbs> RList = new List<Getvillagepbs>();

            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<Getvillagepbs>(query, parameters);
                foreach (var result in results)
                {
                    result.screeningCount = "0";
                    result.uniqueCount = "0";
                    RList.Add(result);
                }
            }
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            if (RList.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "select tbl.village_id,tbl.village_name,tbl.village_gid,count(member_id) TotalCount from(select fm.village_id, H.village_name, H.village_gid, member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_village_master H on H.village_id = fm.village_id group by fm.village_id, H.village_name, H.village_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID " + InstitutionParam + " group by tbl.village_id,tbl.village_name,tbl.village_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getvillagepbs SList = new Getvillagepbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.village_name = RList[i].village_name;
                        SList.village_id = RList[i].village_id;
                        SList.village_gid = RList[i].village_gid;
                        if (SList.village_gid == drInner["village_gid"].ToString())
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

                cmdInner.CommandText = "select tbl.village_id,village_name,village_gid,count(member_id) TotalCount from (select fm.village_id, village_name, village_gid, member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_village_master H on H.village_id = fm.village_id group by fm.village_id, village_name, village_gid,member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.village_id,village_name,village_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getvillagepbs SList = new Getvillagepbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.village_name = RList[i].village_name;
                        SList.village_gid = RList[i].village_gid;
                        if (SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RList[i].uniqueCount = drInner["TotalCount"].ToString();
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

                cmdInner.CommandText = "select tbl.village_id,village_name,village_gid,count(member_id) TotalCount from (select fm.village_id, village_name, village_gid, member_id, count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh inner join family_master fm on hh.family_id = fm.family_id  " + CommunityParam + " inner join address_village_master hd on fm.village_id = hd.village_id where(hh.diseases->0->> 'outcome' = 'Referred out' or hh.diseases->0->> 'outcome' = 'Referred Out') group by fm.village_id, village_name, village_gid, member_id, JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id')tbl INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.village_id,village_name,village_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                Getvillagepbs SList = new Getvillagepbs();
                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {
                        SList.village_name = RList[i].village_name;
                        SList.village_gid = RList[i].village_gid;
                        if (SList.village_gid == drInner["village_gid"].ToString())
                        {
                            RList[i].referredscreening = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            VM.DistrictWise = RList;
            return VM;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetReferredFacility")]
        public async Task<IEnumerable<FacilityModel>> GetReferredFacility([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getreferredfacility('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<FacilityModel>(query);
                return OBJ.ToList();
            }
        }

        //public List<FacilityModel> GetReferredFacility([FromQuery] FilterpayloadModel F)
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    Filterforall(F);

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(screening_id)Count,FR.facility_name from (select JSONB_ARRAY_ELEMENTS(diseases)->> 'outcome' as RR\r\n,screening_id,(diseases)->0->> 'followup_place_id' AS ARRUSER,family_id from health_screening \r\nwhere JSONB_TYPEOF(diseases) = 'array')tbl inner join family_master fm on fm.family_id=tbl.family_id   " + CommunityParam + " INNER JOIN FACILITY_REGISTRY FR ON CAST(FR.FACILITY_ID AS text)=CAST(tbl.ARRUSER AS text)   " + InstitutionParam + "\r\nwhere (rr='Referred out' or rr='Referred Out') group by FR.facility_name order by Count desc limit 6";

        //    List<FacilityModel> Flist = new List<FacilityModel>();

        //    NpgsqlDataReader dr = cmd.ExecuteReader();


        //    while (dr.Read())
        //    {

        //        var SList = new FacilityModel();


        //        SList.facility_name = dr["facility_name"].ToString();
        //        SList.count = dr["count"].ToString();

        //        Flist.Add(SList);
        //    }

        //    con.Close();



        //    return Flist;
        //}




        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getuserperdistrictwise")]
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

            cmd.CommandText = "select * from public.getuserperdistrictwise()";
            //cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid from public.address_district_master as MS order by district_name";

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_1()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date group by M.district_id";

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_2()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-48 hours' < S.last_update_date group by M.district_id";

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_3()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-30 day' < S.last_update_date group by M.district_id";

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


                            RList[i].syncedAverage = (Convert.ToUInt32(T_SyncedScreenings30) / 30).ToString();
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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_4()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date\r\ngroup by S.member_id,M.district_id";

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_5()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere  now()+ interval '-30 day' < S.last_update_date\r\ngroup by S.member_id,M.district_id";

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

                            RList[i].individualAverage = (Convert.ToUInt32(RList[i].individualscreenings30) / 30).ToString();
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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_6()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date\r\ngroup by S.family_id,M.district_id";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date\r\ngroup by S.family_id,M.district_id";

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_7()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-30 day' < S.last_update_date\r\ngroup by S.family_id,M.district_id";

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

                            RList[i].familyscreeningsAverage = (Convert.ToUInt32(RList[i].familyscreenings30) / 30).ToString();

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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_8()";
                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere  S.drugs!='null' and now()+ interval '-24 hours' < S.last_update_date\r\ngroup by M.district_id";

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
                cmdInner.CommandText = "select * from public.getuserperdistrictwise_9()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere S.drugs!='null' and now()+ interval '-30 day' < S.last_update_date\r\ngroup by M.district_id";

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

                            RList[i].drugissuedAverage = (Convert.ToUInt32(RList[i].drugissued30) / 30).ToString();
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
                cmdInner.CommandText = "select * from public.getuserperdistrictwise_10()";

                //cmdInner.CommandText = "select count(S.screening_id)as Count,D.district_name,D.district_id from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_district_master as D on D.district_id = M.district_id\r\nwhere S.drugs !='null'\r\ngroup by D.district_name,D.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].drugcount = drInner["Count"].ToString();
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

                cmdInner.CommandText = "select * from public.getuserperdistrictwise_11()";

                //cmdInner.CommandText = "select count(S.medical_history_id)as Count,D.district_name,D.district_id from public.health_history as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_district_master as D on D.district_id = M.district_id\r\nwhere S.mtm_beneficiary is not null\r\ngroup by D.district_name,D.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].mtmcount = drInner["Count1"].ToString();
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
            VM.syncedAverage = (Convert.ToInt32(VM.T_SyncedScreenings30) / 30).ToString();




            return VM;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getuserperBlockwise")]
        public VMUserPerformance GetUserPerformanceBlock()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMUserPerformance VM = new VMUserPerformance();

            int IN24 = 0;
            int IN30 = 0;
            int FS24 = 0;
            int FS30 = 0;
            int DI24 = 0;
            int DI30 = 0;


            double T_SyncedScreenings24 = 0;
            int T_SyncedScreenings48 = 0;
            int T_SyncedScreenings30 = 0;


            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "select * from public.getuserperblockwise_1()";

            //cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid,BM.block_name,BM.block_gid,HM.hud_name,HM.hud_gid from address_district_master as MS \r\ninner join address_block_master as BM on BM.district_id = MS.district_id\r\ninner join address_hud_master HM on HM.hud_id = BM.hud_id\r\norder by district_name";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<UserPerformanceModel> RList = new List<UserPerformanceModel>();

            while (dr.Read())
            {

                var SList = new UserPerformanceModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();

                SList.block_name = dr["block_name"].ToString();
                SList.block_gid = dr["block_gid"].ToString();

                SList.hud_name = dr["hud_name"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();


                RList.Add(SList);
            }


            con.Close();



            con.Open();

            if (RList.Count > 0)
            {


                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;


                cmdInner.CommandText = "select * from public.getuserperblockwise_2()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening24 = drInner["count"].ToString();

                            T_SyncedScreenings24 = Convert.ToDouble(T_SyncedScreenings24 + double.Parse(RList[i].syncedscreening24));

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

                cmdInner.CommandText = "select * from public.getuserperblockwise_3()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-48 hours' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening48 = drInner["count"].ToString();

                            T_SyncedScreenings48 = Convert.ToInt32(T_SyncedScreenings48 + Convert.ToInt32(RList[i].syncedscreening48));

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


                cmdInner.CommandText = "select * from public.getuserperblockwise_4()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-30 day' < S.last_update_date group by M.district_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].syncedscreening30 = drInner["count"].ToString();

                            T_SyncedScreenings30 = Convert.ToInt32(T_SyncedScreenings30 + Convert.ToInt32(RList[i].syncedscreening30));
                            RList[i].syncedAverage = (Convert.ToUInt32(T_SyncedScreenings30) / 30).ToString();
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

                cmdInner.CommandText = "select * from public.getuserperblockwise_5()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date\r\ngroup by S.member_id,M.district_id";

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



                cmdInner.CommandText = "select * from public.getuserperblockwise_6()";


                //cmdInner.CommandText = "SELECT  count(screening_id),S.member_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere  now()+ interval '-30 day' < S.last_update_date\r\ngroup by S.member_id,M.district_id";

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
                            RList[i].individualAverage = (Convert.ToUInt32(RList[i].individualscreenings30) / 30).ToString();
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


                cmdInner.CommandText = "select * from public.getuserperblockwise_7()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-24 hours' < S.last_update_date\r\ngroup by S.family_id,M.district_id";

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

                cmdInner.CommandText = "select * from public.getuserperblockwise_8()";

                //cmdInner.CommandText = "SELECT  count(screening_id),S.family_id,M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere now()+ interval '-30 day' < S.last_update_date\r\ngroup by S.family_id,M.district_id";

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

                            RList[i].familyscreeningsAverage = (Convert.ToUInt32(RList[i].familyscreenings30) / 30).ToString();

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

                cmdInner.CommandText = "select * from public.getuserperblockwise_9()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere  S.drugs!='null' and now()+ interval '-24 hours' < S.last_update_date\r\ngroup by M.district_id";

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


                cmdInner.CommandText = "select * from public.getuserperblockwise_10()";

                //cmdInner.CommandText = "SELECT  count(screening_id),M.district_id from health_screening S inner join family_member_master M on M.member_id = S.member_id\r\nwhere S.drugs!='null' and now()+ interval '-30 day' < S.last_update_date\r\ngroup by M.district_id";

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

                            RList[i].drugissuedAverage = (Convert.ToUInt32(RList[i].drugissued30) / 30).ToString();

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

                cmdInner.CommandText = "select * from public.getuserperblockwise_11()";

                //cmdInner.CommandText = "select count(S.screening_id)as Count,BL.block_gid,BL.block_name from public.health_screening as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_district_master as D on D.district_id = M.district_id\r\ninner join public.address_block_master BL on BL.district_id = D.district_id\r\nwhere S.drugs !='null'\r\ngroup by BL.block_gid,BL.block_name";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].drugcount = drInner["Count"].ToString();
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


                cmdInner.CommandText = "select * from public.getuserperblockwise_12()";

                //cmdInner.CommandText = "select count(S.medical_history_id)as Count,BM.block_name,BM.block_gid from public.health_history as S \r\ninner join public.family_member_master as M on M.member_id=S.member_id\r\ninner join public.address_district_master as D on D.district_id = M.district_id\r\ninner join public.address_block_master as BM on BM.district_id = D.district_id\r\nwhere S.mtm_beneficiary is not null\r\ngroup by BM.block_name,BM.block_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                UserPerformanceModel SList = new UserPerformanceModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_gid == drInner["block_gid"].ToString())
                        {
                            RList[i].mtmcount = drInner["Count"].ToString();
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
            VM.syncedAverage = (Convert.ToInt32(VM.T_SyncedScreenings30) / 30).ToString();
            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getpopulationkpidashboard")]
        public async Task<VMPopulationKPIModel> GetPopulationKPI()
        {


            VMPopulationKPIModel VM = new VMPopulationKPIModel();

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_totPop() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.total_population = result;

            }


            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_VerPop() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.verified_population = result;

            }

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_UnVerPop() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.unverified_population = result;

            }

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_MigrPop() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.migrated_population = result;

            }

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_NonTrc() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.nontraceable = result;

            }

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_Dupl() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.duplicate = result;

            }

            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_Death() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.death = result;

            }


            using (var connection = _context.CreateConnection())
            {
                string query = "select * from public.Getpopulationkpidashboard_resdPop() ";
                var result = await connection.QueryFirstOrDefaultAsync<string>(query);
                VM.resident_population = result;

            }





            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetKPIDistrictWise")]
        public async Task<List<GetKPIDistrictWise>> GetPopulationKPIDistrictWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            GetKPIDistrictWise VM = new GetKPIDistrictWise();

            string query = "select * from public.GetKPIDistrictWise()";


            List<GetKPIDistrictWise> RList = new List<GetKPIDistrictWise>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<GetKPIDistrictWise>(query);
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_TotPopl()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].total_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_VerPopl() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_UnVerPopl() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_ResdPopl() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_MigrPopl() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_NonTrac()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_Duplicate() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_Death() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].death = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_Consent() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].consent = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.GetKPIDistrictWise_AllocStreet() ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIDistrictWise SList = new GetKPIDistrictWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].allocated_streets = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetKPIHUDWise")]
        public async Task<List<GetKPIHUDWise>> GetPopulationKPIHUDWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            GetKPIHUDWise VM = new GetKPIHUDWise();

            string query = "select * from public.GetKPIHUDWise()";


            List<GetKPIHUDWise> RList = new List<GetKPIHUDWise>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<GetKPIHUDWise>(query);
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_2()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id from family_member_master P inner join address_hud_master D on D.district_id=P.district_id group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].total_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_3()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'resident_details'='Verified' group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_4()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'resident_details'='Unverified' group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "SELECT * from public.getkpihudwise_5()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_6()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id  from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'status'= 'Migrant'  or resident_status_details->>'status'='Migrated out'  group by D.hud_id ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_7()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id  from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'status'= 'Non traceable'  or resident_status_details->>'status'='Non-traceable' group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_8()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id  from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'status'='Duplicate' group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_9()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id  from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death' group by D.hud_id ";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].death = drInner["count"].ToString();
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

                cmdInner.CommandText = "SELECT * from public.getkpihudwise_10()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where consent_status='RECEIVED' group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].consent = drInner["count"].ToString();
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


                cmdInner.CommandText = "SELECT * from public.getkpihudwise_11()";

                //cmdInner.CommandText = "select count(member_id),D.hud_id from family_member_master P inner join address_hud_master D on D.district_id=P.district_id where street_id is not null group by D.hud_id";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                GetKPIHUDWise SList = new GetKPIHUDWise();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].allocated_streets = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getfieldverificationresidentwise")]
        public VMGetfieldverificationresidentwiseModel GetFieldresidentwise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetfieldverificationresidentwiseModel VM = new VMGetfieldverificationresidentwiseModel();


            Filterforall(F);


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;
            //cmdInner.CommandText = "select count(member_id) from family_member_master where resident_status='Resident' " + CommunityParam + "";

            cmdInner.CommandText = "SELECT * FROM public.getfieldverificationresidentwise(@CommunityParam)";
            cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.total_population = drInner["count"].ToString();
            }

            con.Close();

            con.Open();

            NpgsqlCommand cmdnontraceable = new NpgsqlCommand();
            cmdnontraceable.Connection = con;
            cmdnontraceable.CommandType = CommandType.Text;
            //cmdnontraceable.CommandText = "select count(member_id) from family_member_master fm where resident_status_details->> 'status' = 'Non traceable'  or resident_status_details->> 'status' = 'Non-traceable'  " + CommunityParam + "";

            cmdnontraceable.CommandText = "SELECT * FROM public.getfieldverificationresidentwise_1(@CommunityParam)";
            cmdnontraceable.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drnontraceable = cmdnontraceable.ExecuteReader();


            while (drnontraceable.Read())
            {
                VM.nontraceable = drnontraceable["count"].ToString();
            }

            con.Close();


            con.Open();

            NpgsqlCommand cmdDeath = new NpgsqlCommand();
            cmdDeath.Connection = con;
            cmdDeath.CommandType = CommandType.Text;
            //cmdDeath.CommandText = "select count(member_id) from family_member_master fm where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death'  " + CommunityParam + "";

            cmdDeath.CommandText = "SELECT * FROM public.getfieldverificationresidentwise_2(@CommunityParam)";
            cmdDeath.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drDeath = cmdDeath.ExecuteReader();


            while (drDeath.Read())
            {
                VM.death = drDeath["count"].ToString();
            }

            con.Close();

            con.Open();

            NpgsqlCommand cmdVisitor = new NpgsqlCommand();
            cmdVisitor.Connection = con;
            cmdVisitor.CommandType = CommandType.Text;
            //cmdVisitor.CommandText = "select count(member_id) from family_member_master fm where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Visitor'  " + CommunityParam + "";

            cmdVisitor.CommandText = "SELECT * FROM public.getfieldverificationresidentwise_3(@CommunityParam)";
            cmdVisitor.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drVisitor = cmdVisitor.ExecuteReader();

            while (drVisitor.Read())
            {
                VM.visitor = drVisitor["count"].ToString();
            }

            con.Close();

            return VM;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationTotalVerified")]
        public FVTotalPopulationModel GetFieldVTotalVerified([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();

            Filterforall(F);

            FVTotalPopulationModel VM = new FVTotalPopulationModel();
            NpgsqlCommand cmdVerified = new NpgsqlCommand();
            cmdVerified.Connection = con;
            cmdVerified.CommandType = CommandType.Text;
            //cmdVerified.CommandText = "select count(member_id) from family_member_master fm where resident_status_details->>'resident_details'='Verified' " + CommunityParam + "";
            cmdVerified.CommandText = "SELECT * FROM public.getfieldverificationtotalverified(@CommunityParam)";
            cmdVerified.Parameters.AddWithValue("CommunityParam", CommunityParam);
            NpgsqlDataReader drVerified = cmdVerified.ExecuteReader();
            while (drVerified.Read())
            {
                VM.VerifiedPopulation = drVerified["count"].ToString();
            }
            con.Close();
            return VM;
        }



        [HttpPost]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationFamilyMembermonthwise")]
        public FVFamilyMemberPopulationModel GetFieldVFamilyMembermonthwise(FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();
            Filterforall(F);

            FVFamilyMemberPopulationModel VM = new FVFamilyMemberPopulationModel();
            NpgsqlCommand cmdFamily = new NpgsqlCommand();
            cmdFamily.Connection = con;
            cmdFamily.CommandType = CommandType.Text;
            cmdFamily.CommandText = "select to_char(Weekly, 'mon dd') as Months,Count from(SELECT DATE_TRUNC('week',(update_register->0->>'timestamp')::timestamp::date) AS  weekly,COUNT(family_id) AS count FROM public.family_master where update_register->0->>'user_id'!='system'  " + CommunityParam + " GROUP BY weekly order by weekly desc limit 6)tbl";
            NpgsqlDataReader drFamily = cmdFamily.ExecuteReader();
            while (drFamily.Read())
            {
                VM.FamiliesAdded = drFamily["count"].ToString();
            }
            con.Close();

            con.Open();

            NpgsqlCommand cmdMember = new NpgsqlCommand();
            cmdMember.Connection = con;
            cmdMember.CommandType = CommandType.Text;
            cmdMember.CommandText = "select to_char(Weekly, 'mon dd') as Months,Count from(SELECT DATE_TRUNC('week',(update_register->0->>'timestamp')::timestamp::date) AS  weekly,COUNT(member_id) AS count FROM public.family_member_master where update_register->0->>'user_id'!='system' " + CommunityParam + " GROUP BY weekly order by weekly desc limit 6)tbl";
            NpgsqlDataReader drMember = cmdMember.ExecuteReader();
            while (drMember.Read())
            {
                VM.MembersAdded = drMember["count"].ToString();
            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationFamilyMember")]
        public FVFamilyMemberPopulationModel GetFieldVFamilyMember([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            con.Open();
            Filterforall(F);

            FVFamilyMemberPopulationModel VM = new FVFamilyMemberPopulationModel();
            NpgsqlCommand cmdFamily = new NpgsqlCommand();
            cmdFamily.Connection = con;
            cmdFamily.CommandType = CommandType.Text;
            //cmdFamily.CommandText = "select count(family_id)count from family_master fm where update_register->0->>'user_id'!='system'  " + CommunityParam + "";
            cmdFamily.CommandText = "SELECT * FROM public.getfieldverificationfamilymember(@CommunityParam)";
            cmdFamily.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drFamily = cmdFamily.ExecuteReader();
            while (drFamily.Read())
            {
                VM.FamiliesAdded = drFamily["count"].ToString();
            }
            con.Close();

            con.Open();

            NpgsqlCommand cmdMember = new NpgsqlCommand();
            cmdMember.Connection = con;
            cmdMember.CommandType = CommandType.Text;
            //cmdMember.CommandText = "select count(member_id) from family_member_master fm where update_register->0->>'user_id'!='system'  " + CommunityParam + "";
            cmdMember.CommandText = "SELECT * FROM public.getfieldverificationfamilymember_1(@CommunityParam)";
            cmdMember.Parameters.AddWithValue("CommunityParam", CommunityParam);

            NpgsqlDataReader drMember = cmdMember.ExecuteReader();
            while (drMember.Read())
            {
                VM.MembersAdded = drMember["count"].ToString();
            }

            con.Close();

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationDistrictWise")]
        public List<VMGetfieldverificationdistrictwiseModel> GetfieldverificationDistrictWise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetfieldverificationdistrictwiseModel VM = new VMGetfieldverificationdistrictwiseModel();


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select district_id,district_gid,district_name from address_district_master";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMGetfieldverificationdistrictwiseModel> RList = new List<VMGetfieldverificationdistrictwiseModel>();

            while (dr.Read())
            {

                var SList = new VMGetfieldverificationdistrictwiseModel();

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
                //cmdInner.CommandText = "select count(member_id),D.district_id from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id  " + CommunityParam + " group by D.district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].total_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'resident_details'='Verified'  " + CommunityParam + " group by D.district_id  ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_1(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
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
                //cmdInner.CommandText = "select count(member_id),D.district_id from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'resident_details'='Unverified'  " + CommunityParam + " group by D.district_id ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_2(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);



                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id  from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'status'='Resident' " + CommunityParam + " group by D.district_id ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_3(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id  from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'status'= 'Migrant'  or resident_status_details->>'status'='Migrated out'  " + CommunityParam + " group by D.district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_4(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id  from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'status'= 'Non traceable'  or resident_status_details->>'status'='Non-traceable'  " + CommunityParam + " group by D.district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_5(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id  from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'status'='Duplicate'  " + CommunityParam + " group by D.district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_6(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),D.district_id  from family_member_master fm inner join address_district_master D on D.district_id=fm.district_id where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death'  " + CommunityParam + " group by D.district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_7(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].death = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),district_id from family_member_master fm where resident_status_details->>'status'='Visitor' " + CommunityParam + " group by district_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationdistrictwise_8(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationdistrictwiseModel SList = new VMGetfieldverificationdistrictwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == drInner["district_id"].ToString())
                        {
                            RList[i].visitor = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();

            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationHUDWise")]
        public List<VMGetfieldverificationhudwiseModel> GetfieldverificationHUDWise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetfieldverificationhudwiseModel VM = new VMGetfieldverificationhudwiseModel();


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select D.district_id,D.district_gid,D.district_name,H.hud_id,H.hud_name from address_district_master D inner join address_hud_master H on H.district_id=D.district_id";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMGetfieldverificationhudwiseModel> RList = new List<VMGetfieldverificationhudwiseModel>();

            while (dr.Read())
            {

                var SList = new VMGetfieldverificationhudwiseModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();

                SList.hud_id = dr["hud_id"].ToString();
                SList.hud_name = dr["hud_name"].ToString();


                RList.Add(SList);
            }


            con.Close();





            con.Open();

            if (RList.Count > 0)
            {
                string LocalParam = "";

                if (CommunityParam != "")
                {
                    LocalParam = "Where " + CommunityParam;
                }

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select count(member_id),fm.hud_id from family_member_master fm  " + LocalParam + " group by fm.hud_id";
                Console.Write(LocalParam);
                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise(@LocalParam)";
                cmdInner.Parameters.AddWithValue("LocalParam", LocalParam);
                //cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise('" + LocalParam + "')";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].total_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id from family_member_master fm where resident_status_details->>'resident_details'='Verified' " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_1(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id from family_member_master fm where resident_status_details->>'resident_details'='Unverified' " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_2(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id  from family_member_master fm where resident_status_details->>'status'='Resident' " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_3(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id  from family_member_master fm where resident_status_details->>'status'= 'Migrant'  or resident_status_details->>'status'='Migrated out'  " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_4(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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
                cmdInner.CommandText = "select count(member_id),fm.hud_id  from family_member_master fm where resident_status_details->>'status'= 'Non traceable'  or resident_status_details->>'status'='Non-traceable' " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_5(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id  from family_member_master fm where resident_status_details->>'status'='Duplicate'  " + CommunityParam + " group by fm.hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_6(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.hud_id  from family_member_master fm where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death'  " + CommunityParam + " group by fm.hud_id ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_7(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].death = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),hud_id from family_member_master fm where resident_status_details->>'status'='Visitor' " + CommunityParam + " group by hud_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationhudwise_8(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationhudwiseModel SList = new VMGetfieldverificationhudwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == drInner["hud_id"].ToString())
                        {
                            RList[i].visitor = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationblockWise")]
        public List<VMGetfieldverificationblockwiseModel> GetfieldverificationblockWise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetfieldverificationblockwiseModel VM = new VMGetfieldverificationblockwiseModel();


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select D.district_id,D.district_gid,D.district_name,H.hud_id,H.hud_name,B.block_id,B.block_gid,B.block_name from address_district_master D inner join address_hud_master H on H.district_id=D.district_id inner join address_block_master B on B.district_id=D.district_id";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMGetfieldverificationblockwiseModel> RList = new List<VMGetfieldverificationblockwiseModel>();

            while (dr.Read())
            {

                var SList = new VMGetfieldverificationblockwiseModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();

                SList.hud_id = dr["hud_id"].ToString();
                SList.hud_name = dr["hud_name"].ToString();


                SList.block_id = dr["block_id"].ToString();
                SList.block_gid = dr["block_gid"].ToString();
                SList.block_name = dr["block_name"].ToString();


                RList.Add(SList);
            }


            con.Close();





            con.Open();

            if (RList.Count > 0)
            {
                string LocalParam = "";

                if (CommunityParam != "")
                {
                    LocalParam = "Where " + CommunityParam;
                }

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select count(member_id),fm.block_id from family_member_master fm  " + LocalParam + " group by fm.block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise(@LocalParam)";
                cmdInner.Parameters.AddWithValue("LocalParam", LocalParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].total_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id from family_member_master fm where resident_status_details->>'resident_details'='Verified' " + CommunityParam + " group by fm.block_id";


                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_1(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id from family_member_master fm where resident_status_details->>'resident_details'='Unverified' " + CommunityParam + " group by fm.block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_2(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id  from family_member_master fm where resident_status_details->>'status'='Resident' " + CommunityParam + " group by fm.block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_3(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id  from family_member_master fm where resident_status_details->>'status'= 'Migrant'  or resident_status_details->>'status'='Migrated out'  " + CommunityParam + " group by fm.block_id";;

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_4(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id  from family_member_master fm where resident_status_details->>'status'= 'Non traceable'  or resident_status_details->>'status'='Non-traceable' " + CommunityParam + " group by fm.block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_5(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id  from family_member_master fm where resident_status_details->>'status'='Duplicate'  " + CommunityParam + " group by fm.block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_6(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.block_id  from family_member_master fm where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death'  " + CommunityParam + " group by fm.block_id ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_7(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].death = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),block_id from family_member_master fm where resident_status_details->>'status'='Visitor' " + CommunityParam + " group by block_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationblockwise_8(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationblockwiseModel SList = new VMGetfieldverificationblockwiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == drInner["block_id"].ToString())
                        {
                            RList[i].visitor = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetfieldverificationvillageWise")]
        public List<VMGetfieldverificationvillagewiseModel> GetfieldverificationvillageWise([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetfieldverificationvillagewiseModel VM = new VMGetfieldverificationvillagewiseModel();


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select D.district_id,D.district_gid,D.district_name,H.hud_id,H.hud_name,B.block_id,B.block_gid,B.block_name,V.village_id,V.village_gid,V.village_name from address_district_master D inner join address_hud_master H on H.district_id=D.district_id inner join address_block_master B on B.district_id=D.district_id inner join address_village_master V on V.block_id=B.block_id";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VMGetfieldverificationvillagewiseModel> RList = new List<VMGetfieldverificationvillagewiseModel>();

            while (dr.Read())
            {

                var SList = new VMGetfieldverificationvillagewiseModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.district_id = dr["district_id"].ToString();

                SList.hud_id = dr["hud_id"].ToString();
                SList.hud_name = dr["hud_name"].ToString();

                SList.block_gid = dr["block_gid"].ToString();
                SList.block_name = dr["block_name"].ToString();


                SList.village_id = dr["village_id"].ToString();
                SList.village_gid = dr["village_gid"].ToString();
                SList.village_name = dr["village_name"].ToString();


                RList.Add(SList);
            }


            con.Close();





            con.Open();

            if (RList.Count > 0)
            {
                string LocalParam = "";

                if (CommunityParam != "")
                {
                    LocalParam = "Where " + CommunityParam;
                }

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select count(member_id),fm.village_id from family_member_master fm  " + LocalParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise(@LocalParam)";
                cmdInner.Parameters.AddWithValue("LocalParam", LocalParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].total_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id from family_member_master fm where resident_status_details->>'resident_details'='Verified' " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_1(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id from family_member_master fm where resident_status_details->>'resident_details'='Unverified' " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_2(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].unverified_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id  from family_member_master fm where resident_status_details->>'status'='Resident' " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_3(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].resident_population = drInner["count"].ToString();
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
                // cmdInner.CommandText = "select count(member_id),fm.village_id  from family_member_master fm where resident_status_details->>'status'= 'Migrant'  or resident_status_details->>'status'='Migrated out'  " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_4(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].migrated_population = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id  from family_member_master fm where resident_status_details->>'status'= 'Non traceable'  or resident_status_details->>'status'='Non-traceable' " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_5(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].nontraceable = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id  from family_member_master fm where resident_status_details->>'status'='Duplicate'  " + CommunityParam + " group by fm.village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_6(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].duplicate = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),fm.village_id  from family_member_master fm where resident_status_details->>'status'= 'Dead'  or resident_status_details->>'status'='Death'  " + CommunityParam + " group by fm.village_id ";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_7(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].death = drInner["count"].ToString();
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
                //cmdInner.CommandText = "select count(member_id),village_id from family_member_master fm where resident_status_details->>'status'='Visitor' " + CommunityParam + " group by village_id";

                cmdInner.CommandText = "SELECT * FROM public.getfieldverificationvillagewise_8(@CommunityParam)";
                cmdInner.Parameters.AddWithValue("CommunityParam", CommunityParam);

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                VMGetfieldverificationvillagewiseModel SList = new VMGetfieldverificationvillagewiseModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].village_id == drInner["village_id"].ToString())
                        {
                            RList[i].visitor = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }




        /*Looker Filter*/

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getdistrictmasterlooker")]
        public VMCommunityTriage getdistrictmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_name,MS.district_gid,MS.district_id from \r\npublic.address_district_master as MS ";

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
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gethudmasterlooker")]
        public VMCommunityTriage gethudmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select MS.district_name,hu.hud_id,hu.hud_gid,hu.HUD_name,MS.district_gid,MS.district_id from \r\npublic.address_district_master as MS \r\ninner join public.address_hud_master as hu on MS.district_id=hu.district_id";

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
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getblockmasterlooker")]
        public VMCommunityTriage getblockmaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select bm.*,hm.hud_gid,hud_name,district_gid,district_name from address_block_master bm inner join address_hud_master hm on bm.hud_id=hm.hud_id inner join address_district_master adm on adm.district_id=bm.district_id";

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

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getrolemasterlooker")]
        public List<RoleModel> getRolemaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select role_id,role_name from user_role_master order by role_name";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<RoleModel> RList = new List<RoleModel>();

            while (dr.Read())
            {

                var SList = new RoleModel();

                SList.role_id = dr["role_id"].ToString();
                SList.role_name = dr["role_name"].ToString();


                RList.Add(SList);
            }
            con.Close();

            return RList;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getdirectoratemasterlooker")]
        public List<DirectorateModel> getdirectoratemaster()
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select directorate_id,directorate_name from facility_directorate_master";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<DirectorateModel> RList = new List<DirectorateModel>();

            while (dr.Read())
            {

                var SList = new DirectorateModel();

                SList.directorate_id = dr["directorate_id"].ToString();
                SList.directorate_name = dr["directorate_name"].ToString();


                RList.Add(SList);
            }
            con.Close();

            return RList;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getfacilitymasterlooker")]
        public List<FacilityModel> getfacilitymaster(string facility_name)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select facility_id,facility_name from facility_registry where facility_name like '%" + facility_name + "%'  limit 100";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<FacilityModel> RList = new List<FacilityModel>();

            while (dr.Read())
            {

                var SList = new FacilityModel();

                SList.facility_id = dr["facility_id"].ToString();
                SList.facility_name = dr["facility_name"].ToString();


                RList.Add(SList);
            }
            con.Close();

            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getvillagemasterlooker")]
        public List<VillageMasterModel> getvillagemaster(string village_name)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select village_name,village_id from address_village_master where village_name like '%" + village_name + "%' limit 100";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VillageMasterModel> RList = new List<VillageMasterModel>();

            while (dr.Read())
            {

                var SList = new VillageMasterModel();

                SList.village_id = dr["village_id"].ToString();
                SList.village_name = dr["village_name"].ToString();


                RList.Add(SList);
            }
            con.Close();

            return RList;
        }


        /*Data Quality*/

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamiliesstreetunallocated")]

        public async Task<IEnumerable<DataQuality>> Familiesstreetunallocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.GetFamiliesstreetunallocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ;
            }
        }

        //public string Familiesstreetunallocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(family_id) from family_master where street_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamiliesfacilityunallocated")]
        public async Task<IEnumerable<DataQuality>> GetFamiliesfacilityunallocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.GetFamiliesfacilityunallocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string Familiesfacilityunallocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(family_id) from family_master where facility_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamilieswithnull")]

        public async Task<IEnumerable<DataQuality>> GetFamilieswithnull([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.GetFamilieswithnull()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string Familieswithnull()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(family_id) from family_master where shop_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamilieswithmore")]
        public async Task<IEnumerable<DataQuality>> GetFamilieswithmore([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.GetFamilieswithmore()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string Familieswithmore()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(family_id) from family_master where family_members_count>=10";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmemberswithstreetsunallocated")]

        public async Task<IEnumerable<DataQuality>> Getmemberswithstreetsunallocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.Getmemberswithstreetsunallocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string memberswithstreetsunallocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where street_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmemberswithfacilityunallocated")]

        public async Task<IEnumerable<DataQuality>> Getmemberswithfacilityunallocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.Getmemberswithfacilityunallocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string memberswithfacilityunallocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where facility_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmemberswithaadhaar_number")]

        public async Task<IEnumerable<DataQuality>> Getmemberswithaadhaar_number([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.Getmemberswithaadhaar_number()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string memberswithaadhaar_number()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where aadhaar_number is not null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getverifiedmembers")]
        public async Task<IEnumerable<DataQuality>> Getverifiedmembers([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.Getverifiedmembers()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string verifiedmembers()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where resident_status_details->>'resident_details'='Verified'";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getstreetswithfacilityunallocated")]
        public async Task<IEnumerable<DataQuality>> Getstreetswithfacilityunallocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getstreetswithfacilityunallocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string streetswithfacilityunallocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(street_id) from address_street_master where facility_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getshopswithnomappinfstreets")]

        public async Task<IEnumerable<DataQuality>> Getshopswithnomappinfstreets([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getshopswithnomappinfstreets()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string shopswithnomappinfstreets()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(shop_id) from address_shop_master where street_gid is not null ";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmembersinhistorynothavingscreening")]
        public async Task<IEnumerable<DataQuality>> Getmembersinhistorynothavingscreening([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getmembersinhistorynothavingscreening()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string membersinhistorynothavingscreening()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(medical_history_id) from health_history h left join health_screening HS on HS.member_id=h.member_id where HS.screening_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmembershavingscreeningnothavinghistory")]
        public async Task<IEnumerable<DataQuality>> Getmembershavingscreeningnothavinghistory([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getmembershavingscreeningnothavinghistory()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string membershavingscreeningnothavinghistory()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(screening_id) from health_screening  h left join health_history HS on HS.member_id=h.member_id where HS.medical_history_id is null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmobilenumbermorethan")]
        public async Task<IEnumerable<DataQuality>> Getmobilenumbermorethan([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getmobilenumbermorethan()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string mobilenumbermorethan()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select sum(cnt) count from(select count(mobile_number) cnt,mobile_number from family_member_master where  LENGTH(mobile_number::text)>=10 group by mobile_number having count(mobile_number) >10 order by mobile_number desc)tbl";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getmemberswithmobilenumber")]

        public async Task<IEnumerable<DataQuality>> Getmemberswithmobilenumber([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getmemberswithmobilenumber()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string memberswithmobilenumber()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where LENGTH(mobile_number::text)>=10";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("Getpopulationmappedwithstreet")]
        public async Task<IEnumerable<DataQuality>> Getpopulationmappedwithstreet([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.Getpopulationmappedwithstreet()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string populationmappedwithstreet()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where street_id is not null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetMembersmappedtoUnallocatedfacility")]

        public async Task<IEnumerable<DataQuality>> GetMembersmappedtoUnallocatedfacility([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.GetMembersmappedtoUnallocatedfacility()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string MembersmappedtoUnallocatedfacility()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where street_id is not null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetMemberswithstreetasUnAllocated")]
        public async Task<IEnumerable<DataQuality>> GetMemberswithstreetasUnAllocated([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            var query = "SELECT * from public.GetMemberswithstreetasUnAllocated()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<DataQuality>(query);
                return OBJ.ToList();
            }
        }
        //public string MemberswithstreetasUnAllocated()
        //{
        //    NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

        //    con.Open();
        //    NpgsqlCommand cmd = new NpgsqlCommand();
        //    cmd.Connection = con;
        //    cmd.CommandType = CommandType.Text;
        //    cmd.CommandText = "select count(member_id) from family_member_master where street_id is not null";

        //    NpgsqlDataReader dr = cmd.ExecuteReader();

        //    string CountValue = "";

        //    while (dr.Read())
        //    {

        //        CountValue = dr["count"].ToString();

        //    }

        //    con.Close();

        //    return CountValue;
        //}


        [HttpGet]

        [Route("LoginForm")]
        public async Task<string> LoginFlow([FromQuery] loginmodel L)
        {
            var query = "select * from public.lookeruser where mobile='" + L.mobile + "' and password='" + L.password + "'";

            string ResponseMessage = "";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<loginmodel>(query);

                var res = OBJ.ToList();

                if (res.Count > 0)
                {
                    ResponseMessage = "Valid";



                    var secretKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("secretkey for oyasys phr to hims integration"));
                    var signinCredentials = new SigningCredentials(secretKey, SecurityAlgorithms.HmacSha512Signature);
                    var tokeOptions = new JwtSecurityToken(
                        issuer: "http://142.132.206.93:9060/",
                        audience: "http://142.132.206.93:9060/",
                        claims: new List<Claim>(),
                        expires: DateTime.Now.AddMinutes(5),
                        signingCredentials: signinCredentials
                    );
                    var tokenString = new JwtSecurityTokenHandler().WriteToken(tokeOptions);
                    return tokenString;

                }
                else
                {
                    ResponseMessage = "Invalid";
                }
            }

            return ResponseMessage;
        }



        /*Service Monitoring Start*/
        [HttpGet]
        [Route("getDrugQuantity")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getDrugQuantity()
        {

            var query = "SELECT * from public.ServiceMon_DrugQuantity()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }


        [HttpGet]
        [Route("getLabTestsinlast")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getLabTestsinlast()
        {

            var query = "SELECT * from public.ServiceMon_LabTest()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }


        [HttpGet]
        [Route("getStreetswithundelivered")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getStreetswithundelivered()
        {

            var query = "SELECT * from public.ServiceMon_Streetswithundelivered()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        [Route("getServiceMon_Noofstreetswithservicesdelivered")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getServiceMon_Noofstreetswithservicesdelivered()
        {

            var query = "SELECT * from public.ServiceMon_Noofstreetswithservicesdelivered()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        [Route("getservicemon_streetswithundelivered90")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getservicemon_streetswithundelivered90()
        {

            var query = "SELECT * from public.servicemon_streetswithundelivered90()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        [Route("getServiceMon_ScreeningPeruser")]
        public async Task<IEnumerable<ServiceMonitoringModelcs>> getServiceMon_ScreeningPeruser()
        {

            var query = "SELECT * from public.ServiceMon_ScreeningPeruser()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<ServiceMonitoringModelcs>(query);
                return OBJ.ToList();
            }
        }

        [HttpGet]
        [Route("getServiceMon_StreetwithScreeningcount")]
        public async Task<IEnumerable<StreetsScreeningCountModel>> getServiceMon_StreetwithScreeningcount()
        {

            var query = "SELECT * from public.ServiceMon_StreetwithScreeningcount()";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<StreetsScreeningCountModel>(query);
                return OBJ.ToList();
            }
        }

        /*District*/


        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetServicesMonitoringDistrictWise")]
        public async Task<List<VMServiceMonitoringDistrict>> GetServicesMonitoringDistrictWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMServiceMonitoringDistrict VM = new VMServiceMonitoringDistrict();

            string query = "select * from public.GetKPIDistrictWise()";


            List<VMServiceMonitoringDistrict> RList = new List<VMServiceMonitoringDistrict>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<VMServiceMonitoringDistrict>(query);
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
                cmdInner.CommandText = "select * from public.servicemon_totalscreening_D()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].totalscreening = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_totallab_D()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].totallabtest = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_labtest_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].labtest30 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_screeningperuser_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].screeningperuser = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].streetswithundelivered = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered90_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].streetswithundelivered90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_noofstreetswithservicesdelivered_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].streetswithnoscreenings90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithdelivered_d()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].district_id == Guid.Parse(drInner["district_id"].ToString()))
                        {
                            RList[i].streetswithservicesdelivered = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();





            return RList;
        }


        /*HUD*/

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetServicesMonitoringHUDWise")]
        public async Task<List<VMServiceMonitoringHUD>> GetServicesMonitoringHUDWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMServiceMonitoringHUD VM = new VMServiceMonitoringHUD();

            string query = "select * from public.getkpihudwise()";


            List<VMServiceMonitoringHUD> RList = new List<VMServiceMonitoringHUD>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<VMServiceMonitoringHUD>(query);
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
                cmdInner.CommandText = "select * from public.servicemon_totalscreening_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].totalscreening = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_totallab_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].totallabtest = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_labtest_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].labtest30 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_screeningperuser_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].screeningperuser = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].streetswithundelivered = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered90_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].streetswithundelivered90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_noofstreetswithservicesdelivered_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].streetswithnoscreenings90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithdelivered_h()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].hud_id == Guid.Parse(drInner["hud_id"].ToString()))
                        {
                            RList[i].streetswithservicesdelivered = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }



        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        //[OutputCache(Duration = 30 * 60)]
        [Route("GetServicesMonitoringBlockWise")]
        public async Task<List<VMServiceMonitoringBlock>> GetServicesMonitoringBlockWise()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMServiceMonitoringBlock VM = new VMServiceMonitoringBlock();

            string query = "select * from public.getblockmaster()";


            List<VMServiceMonitoringBlock> RList = new List<VMServiceMonitoringBlock>();
            using (var connection = _context.CreateConnection())
            {
                var results = await connection.QueryAsync<VMServiceMonitoringBlock>(query);
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
                cmdInner.CommandText = "select * from public.servicemon_totalscreening_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].totalscreening = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_totallab_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].totallabtest = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_labtest_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].labtest30 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_screeningperuser_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].screeningperuser = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].streetswithundelivered = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithundelivered90_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].streetswithundelivered90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_noofstreetswithservicesdelivered_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].streetswithnoscreenings90 = drInner["count"].ToString();
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
                cmdInner.CommandText = "select * from public.servicemon_streetswithdelivered_b()";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();

                while (drInner.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        if (RList[i].block_id == Guid.Parse(drInner["block_id"].ToString()))
                        {
                            RList[i].streetswithservicesdelivered = drInner["count"].ToString();
                        }

                    }
                }

            }


            con.Close();




            return RList;
        }


        /*Service Monitoring End*/

        /*Rural vs Urban Start*/

        /*Rural vs Urban End*/

    }
}
