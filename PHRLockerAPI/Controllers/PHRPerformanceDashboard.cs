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
    [ApiController]
    public class PHRPerformanceDashboard : ControllerBase
    {
        private readonly IConfiguration _configuration;

        private readonly DapperContext _context;
        string CommunityParam = "";
        string InstitutionParam = "";

        private readonly DapperContext context;

        public PHRPerformanceDashboard(DapperContext context, IConfiguration configuration)
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
                            Disparam = Disparam + "fmm.district_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fmm.district_id = '" + v + "' or ";
                        }
                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fmm.district_id = '" + F.district_id + "'";
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
                            Disparam = Disparam + "fmm.hud_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fmm.hud_id = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fmm.hud_id = '" + F.hud_id + "'";
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
                            Disparam = Disparam + "fmm.block_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fmm.block_id = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fmm.block_id = '" + F.block_id + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }
            if (F.block_type != "")
            {
                string Disparam = "";
                string Disparams = "";

                if (F.block_type.Contains(","))
                {
                    int i = 0;
                    string[] blocktypeValue = F.block_type.Split(",");

                    foreach (var v in blocktypeValue)
                    {
                        if (i == (blocktypeValue.Length - 1))
                        {
                            Disparam = Disparam + "abm.block_type = '" + v + "'";
                            Disparams = Disparams + " INNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id";
                        }
                        else
                        {
                            Disparam = Disparam + "abm.block_type = '" + v + "' or ";
                            Disparams = Disparams + " INNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and abm.block_type = '" + F.block_type + "'";
                    Disparams = " INNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id";
                }

                CommunityParam = CommunityParam + Disparam;
                InstitutionParam = InstitutionParam + Disparams;

            }
            if (F.age != "")
            {
                string Disparam = "";

                if (F.age.Contains(","))
                {
                    int i = 0;
                    string[] ageValue = F.age.Split(",");

                    foreach (var v in ageValue)
                    {
                        if (i == (ageValue.Length - 1))
                        {
                            Disparam = Disparam + "date_part('year',age(fmm.birth_date)) = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "date_part('year',age(fmm.birth_date)) = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and date_part('year',age(fmm.birth_date)) = '" + F.age + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }
            if (F.gender != "")
            {
                string Disparam = "";

                if (F.gender.Contains(","))
                {
                    int i = 0;
                    string[] genderValue = F.gender.Split(",");

                    foreach (var v in genderValue)
                    {
                        if (i == (genderValue.Length - 1))
                        {
                            Disparam = Disparam + "fmm.gender = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fmm.gender = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fmm.gender = '" + F.gender + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }
           /* if (F.directorate_id != "")
            {
                string Disparam = "";
                string Disparams = "";

                if (F.directorate_id.Contains(","))
                {
                    int i = 0;
                    string[] directorateValue = F.directorate_id.Split(",");

                    foreach (var v in directorateValue)
                    {
                        if (i == (directorateValue.Length - 1))
                        {
                            Disparam = Disparam + "fr.directorate_id = '" + v + "'";
                            Disparams = Disparams + " INNER JOIN facility_registry as fr ON fmm.facility_id = fr.facility_id";
                        }
                        else
                        {
                            Disparam = Disparam + "fr.directorate_id = '" + v + "' or ";
                            Disparams = Disparams + " INNER JOIN facility_registry as fr ON fmm.facility_id = fr.facility_id";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fr.directorate_id = '" + F.directorate_id + "'";
                    Disparams = " INNER JOIN facility_registry as fr ON fmm.facility_id = fr.facility_id";
                }

                CommunityParam = CommunityParam + Disparam;
                InstitutionParam = InstitutionParam + Disparams;

            }*/
           /* if (F.owner_id != "")
            {
                string Disparam = "";

                if (F.owner_id.Contains(","))
                {
                    int i = 0;
                    string[] ownerValue = F.owner_id.Split(",");

                    foreach (var v in ownerValue)
                    {
                        if (i == (ownerValue.Length - 1))
                        {
                            Disparam = Disparam + "fr.owner_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fr.owner_id = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fr.owner_id = '" + F.owner_id + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }*/
           /* if (F.facility_type_id != "")
            {
                string Disparam = "";

                if (F.facility_type_id.Contains(","))
                {
                    int i = 0;
                    string[] facilitytypeValue = F.facility_type_id.Split(",");

                    foreach (var v in facilitytypeValue)
                    {
                        if (i == (facilitytypeValue.Length - 1))
                        {
                            Disparam = Disparam + "fr.facility_type_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fr.facility_type_id = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fr.facility_type_id = '" + F.facility_type_id + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }*/
           /* if (F.role != "")
            {
                string Disparam = "";
                string Disparams = "";

                if (F.role.Contains(","))
                {
                    int i = 0;
                    string[] roleValue = F.role.Split(",");

                    foreach (var v in roleValue)
                    {
                        if (i == (roleValue.Length - 1))
                        {
                            Disparam = Disparam + "um.role = '" + v + "'";
                            Disparams = " INNER JOIN user_master as um ON fmm.facility_id = um.facility_id";
                        }
                        else
                        {
                            Disparam = Disparam + "um.role = '" + v + "' or ";
                            Disparams = " INNER JOIN user_master as um ON fmm.facility_id = um.facility_id";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and um.role = '" + F.role + "'";
                    Disparams = " INNER JOIN user_master as um ON fmm.facility_id = um.facility_id";
                }

                CommunityParam = CommunityParam + Disparam;
                InstitutionParam = InstitutionParam + Disparams;

            }*/
           /* if (F.service_id != "")
            {
                string Disparam = "";

                if (F.service_id.Contains(","))
                {
                    int i = 0;
                    string[] serviceValue = F.service_id.Split(",");

                    foreach (var v in serviceValue)
                    {
                        if (i == (serviceValue.Length - 1))
                        {
                            Disparam = Disparam + "hdm.service_id = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "hdm.service_id = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and hdm.service_id = '" + F.service_id + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }*/
            /*if (F.facility_level != "")
            {
                string Disparam = "";

                if (F.facility_level.Contains(","))
                {
                    int i = 0;
                    string[] serviceValue = F.facility_level.Split(",");

                    foreach (var v in serviceValue)
                    {
                        if (i == (serviceValue.Length - 1))
                        {
                            Disparam = Disparam + "fr.facility_level = '" + v + "'";
                        }
                        else
                        {
                            Disparam = Disparam + "fr.facility_level = '" + v + "' or ";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and fr.facility_level = '" + F.facility_level + "'";
                }

                CommunityParam = CommunityParam + Disparam;

            }*/
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetTotalPopulationCountPhrPer")]
        public VMGetTotalPopulationPhrPerModel gettotalPopulationcountphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetTotalPopulationPhrPerModel VM = new VMGetTotalPopulationPhrPerModel();

            Filterforall(F);

            string para = "";

            if (CommunityParam.StartsWith("and"))
            {
                para = Regex.Replace(CommunityParam, @"^and", "Where", RegexOptions.IgnoreCase);
            }
            else
            {
                para = CommunityParam;
            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master AS fmm " + InstitutionParam + " " + para + "";

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
        [Route("GetIndividualScreenedGenderWisePhrPer")]
        public GetIndividualScreenedGenderWisePhrPerModel getindividualscreenedgenderwisephrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            GetIndividualScreenedGenderWisePhrPerModel VM = new GetIndividualScreenedGenderWisePhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT\r\n  COUNT(CASE WHEN fmm.gender = 'Male' THEN 1 END) AS maleCount,\r\n  COUNT(CASE WHEN fmm.gender = 'Female' THEN 1 END) AS femaleCount,\r\n  COUNT(CASE WHEN fmm.gender = 'Other' THEN 1 END) AS otherCount\r\nFROM family_member_master fmm\r\nINNER JOIN health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam  + " " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Male_Count = drInner["maleCount"].ToString();
                VM.Female_Count = drInner["femaleCount"].ToString();
                VM.Other_Count = drInner["otherCount"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetMTMTargetUniqueScreenedPhrPer")]
        public VMGetMTMTargetUniqueScreenedPhrPerModel[] getmtmtargetuniquescreenedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetMTMTargetUniqueScreenedPhrPerModel VM = new VMGetMTMTargetUniqueScreenedPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id),\r\n  CASE\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 0 AND 17 THEN 'Below 18'\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 18 AND 45 THEN '18 to 45'\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 46 AND 59 THEN '46 to 59'\r\n\tWHEN date_part('year', age(fmm.birth_date)) >= 60 THEN '60 or Above'\r\n  END AS age_group\r\nfrom family_member_master fmm \r\ninner join health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY age_group";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetMTMTargetUniqueScreenedPhrPerModel> RList = new List<VMGetMTMTargetUniqueScreenedPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetMTMTargetUniqueScreenedPhrPerModel();

                SList.Age_Group = drInner["age_group"].ToString();
                SList.Age_Count = drInner["count"].ToString();

                RList.Add(SList);

            }

            con.Close();

            VMGetMTMTargetUniqueScreenedPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamilyResidentailStatusPhrPer")]
        public VMGetFamilyResidentailStatusPhrPerModel getfamilyresidentailstatusphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetFamilyResidentailStatusPhrPerModel VM = new VMGetFamilyResidentailStatusPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON fm.family_id = fmm.family_id " + InstitutionParam + " ";
            InstitutionParam = InstitutionParams;

            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fm.family_id) FROM family_master as fm " + InstitutionParam + " WHERE fm.reside_status IN ('Permanent','Temporary','Migrated')\r\n " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Family_Residential_Status = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetMembersAddedPhrPer")]
        public VMGetMembersAddedPhrPerModel getmembersaddedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetMembersAddedPhrPerModel VM = new VMGetMembersAddedPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM public.family_member_master as fmm " + InstitutionParam + " WHERE fmm.update_register->0->>'user_id'!='system'" + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Members_Added = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamiliesAddedPhrPer")]
        public VMGetFamiliesAddedPhrPerModel getfamiliesaddedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetFamiliesAddedPhrPerModel VM = new VMGetFamiliesAddedPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON fm.family_id = fmm.family_id " + InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fm.family_id) FROM public.family_master as fm " + InstitutionParam + " WHERE fm.reside_status = 'Permanent'\r\n" + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Families_Added = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFamiliesUpdatedPhrPer")]
        public VMGetFamiliesUpdatedPhrPerModel getfamiliesUpdated ([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetFamiliesUpdatedPhrPerModel VM = new VMGetFamiliesUpdatedPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fm.family_id) \r\nFROM family_master as fm \r\nINNER JOIN family_member_master as fmm ON fm.family_id = fmm.family_id " + InstitutionParam + " WHERE fmm.update_register->0->>'user_id'!='system' \r\nand fmm.update_register->0->>'user_id'!='null' \r\nand fm.reside_status = 'Permanent'\r\n" + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Families_Updated = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationGivenConsentPhrPer")]
        public VMGetPopulationGivenConsentPhrPerModel getpopulationgivenconsentphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPopulationGivenConsentPhrPerModel VM = new VMGetPopulationGivenConsentPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.consent_status = 'RECEIVED'\t " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Population_Given_Consent = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetScreenedOnlyOnceAndMultipleTimesPhrPer")]
        public VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel[] getscreenedonlyonceandmultipletimesphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel VM = new VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.member_id = fmm.member_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "select \r\ncase \r\nwhen family_id is not null then 'repeated'\r\nwhen family_id is null then 'firstTime'\r\nend as test, count(*) as counts, dayss\r\nfrom \r\n(select b.family_id, tbl.member_id, dayss from\r\n(select hs.member_id, count(*) as countss, date_trunc('day', hs.last_update_date) \r\n as dayss from  health_screening as hs " + InstitutionParam + " where hs.last_update_date >= date_trunc('day'\r\n, NOW()) - INTERVAL '7 day' AND  hs.last_update_date < date_trunc('day', NOW()) " + CommunityParam + " GROUP BY hs.member_id, date_trunc('day', hs.last_update_date)) as tbl \r\n left JOIN health_screening b on tbl.member_id = b.member_id \r\n and date_trunc('day', last_update_date) != date_trunc('day', dayss)) as tbls group by \r\ncase \r\nwhen family_id is not null then 'repeated'\r\nwhen family_id is null then 'firstTime'\r\nend, dayss order by dayss desc";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel> RList = new List<VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel();

                SList.Test = drInner["test"].ToString();
                SList.Count = drInner["counts"].ToString();
                SList.Days = drInner["dayss"].ToString();

                RList.Add(SList);


            }

            con.Close();

            VMGetScreenedOnlyOnceAndMultipleTimesPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetNCDTargetUniqueScreenedPhrPer")]
        public VMGetNCDTargetUniqueScreenedPhrPerModel[] getncdtargetuniquescreenedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetNCDTargetUniqueScreenedPhrPerModel VM = new VMGetNCDTargetUniqueScreenedPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT\r\n  COUNT(fmm.member_id),\r\n  CASE\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 0 AND 17 THEN 'Below 18'\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 18 AND 29 THEN '18 to 29'\r\n    WHEN date_part('year', age(birth_date)) >= 30 THEN '30 or Above'\r\n  END AS age_group\r\nFROM family_member_master fmm\r\nINNER JOIN health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY age_group";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetNCDTargetUniqueScreenedPhrPerModel> RList = new List<VMGetNCDTargetUniqueScreenedPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetNCDTargetUniqueScreenedPhrPerModel();

                SList.Age_Group = drInner["age_group"].ToString();
                SList.Age_Count = drInner["count"].ToString();

                RList.Add(SList);

            }

            con.Close();

            VMGetNCDTargetUniqueScreenedPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationMappedStreetsPhrPer")]
        public VMGetPopulationMappedStreetsPhrPerModel getpopulationmappedstreetsphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPopulationMappedStreetsPhrPerModel VM = new VMGetPopulationMappedStreetsPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE street_id is not null " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Population_Mapped_Street = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetCMCHISBeneficiariesPhrPer")]
        public VMGetCMCHISBeneficiariesPhrPerModel getcmchisbeneficiaries([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetCMCHISBeneficiariesPhrPerModel VM = new VMGetCMCHISBeneficiariesPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            
                string InstitutionParams = " INNER JOIN family_member_master as fmm ON fm.family_id = fmm.family_id "+ InstitutionParam + "";
                InstitutionParam = InstitutionParams;
            }



            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT count(*) FROM family_master as fm " + InstitutionParam + " WHERE fm.family_insurances->'insurance'->0->>'id'~ '^\\d+$' AND NOT(fm.family_insurances->'insurance'->0->>'id'~'0')\r\n" + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.CMCHIS_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetCongenitalAnomalyPhrPer")]
        public VMGetCongenitalAnomalyPhrPerModel getcongenitalanomalyphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetCongenitalAnomalyPhrPerModel VM = new VMGetCongenitalAnomalyPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hh.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(hh.medical_history_id) FROM health_history as hh " + InstitutionParam + " WHERE hh.congenital_anomaly = 'True'\r\n" + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Congenital_Anomaly = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetScreenedLastSevenDaysPhrPer")]
        public VMGetScreenedLastSevenDaysPhrPerModel[] getscreenedlastsevendaysphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetScreenedLastSevenDaysPhrPerModel VM = new VMGetScreenedLastSevenDaysPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {
            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;
            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "select count(hs.member_id) as count , date_trunc('day', hs.last_update_date) as dayss \r\nfrom health_screening as hs " + InstitutionParam + " WHERE hs.last_update_date >= date_trunc('day'\r\n, NOW()) - INTERVAL '7 day' AND  hs.last_update_date < date_trunc('day', NOW()) " + CommunityParam + " GROUP BY date_trunc('day', hs.last_update_date)\r\n";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetScreenedLastSevenDaysPhrPerModel> RList = new List<VMGetScreenedLastSevenDaysPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetScreenedLastSevenDaysPhrPerModel();

                SList.Last_Seven_Days = drInner["dayss"].ToString();
                SList.Count = drInner["count"].ToString();

                RList.Add(SList);

            }

            con.Close();

            VMGetScreenedLastSevenDaysPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetSchoolTargetUniqueScreenedPhrPer")]
        public VMGetSchoolTargetUniqueScreeningPhrPerModel[] getschoolargetuniquescreenedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetSchoolTargetUniqueScreeningPhrPerModel VM = new VMGetSchoolTargetUniqueScreeningPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT \r\nCOUNT(fmm.member_id),\r\nCASE \r\nWHEN date_part('year', age(birth_date)) BETWEEN 0 AND 3 THEN 'Below 3'\r\nWHEN date_part('year', age(birth_date)) BETWEEN 3 AND 6 THEN '3 to 6'\r\nWHEN date_part('year', age(birth_date)) BETWEEN 7 AND 9 THEN '7 to 9'\r\nWHEN date_part('year', age(birth_date)) BETWEEN 10 AND 19 THEN '10 to 19'\r\nEND AS age_group\r\nFROM family_member_master fmm\r\nINNER JOIN health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam  + " " + CommunityParam + " GROUP BY age_group\r\n";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetSchoolTargetUniqueScreeningPhrPerModel> RList = new List<VMGetSchoolTargetUniqueScreeningPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetSchoolTargetUniqueScreeningPhrPerModel();

                SList.Age_Group = drInner["age_group"].ToString();
                SList.Age_Count = drInner["count"].ToString();

                RList.Add(SList);

            }

            con.Close();

            VMGetSchoolTargetUniqueScreeningPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetAadhaarLinkedMembersPhrPer")]
        public VMGetAadhaarLinkedMembersPhrPerModel getaadhaarlinkedmembersphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetAadhaarLinkedMembersPhrPerModel VM = new VMGetAadhaarLinkedMembersPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.aadhaar_number is not null\r\n " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Aadhaar_Linked_Member_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetUDIDLinkedMembersPhrPer")]
        public VMGetUDIDLinkedMembersPhrPerModel getudidlinkedmembersphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetUDIDLinkedMembersPhrPerModel VM = new VMGetUDIDLinkedMembersPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hh.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(hh.medical_history_id) FROM health_history as hh " + InstitutionParam + "  WHERE hh.disability_details->>'udid' is not null " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.UDID_Linked_Member_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDisabilityBeneficiariesPhrPer")]
        public VMGetDisabilityBeneficiariesPhrPerModel getdisabilitybeneficiariesphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetDisabilityBeneficiariesPhrPerModel VM = new VMGetDisabilityBeneficiariesPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {
            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hh.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;
            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(hh.medical_history_id) FROM health_history as hh " + InstitutionParam + " WHERE hh.disability = 'True' AND hh.mtm_beneficiary->>'avail_service' = 'yes'  " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Disability_Beneficiaries_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetIndividualScreeningsPhrPer")]
        public VMGetIndividualScreeningsPhrPerModel getindividualscreeningsphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetIndividualScreeningsPhrPerModel VM = new VMGetIndividualScreeningsPhrPerModel();

            Filterforall(F);

            string para = "";

            if (CommunityParam.StartsWith("and"))
            {
                para = Regex.Replace(CommunityParam, @"^and", "Where", RegexOptions.IgnoreCase);
            }
            else
            {
                para = CommunityParam;
            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(DISTINCT fmm.member_id) \r\nFROM health_screening as hs\r\nINNER JOIN family_member_master as fmm ON hs.member_id = fmm.member_id\r\n " + InstitutionParam  + " " + para + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Individual_Screening_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetIndividualReceivedDrugsPhrPer")]
        public VMGetIndividualReceivedDrugsPhrPerModel getindividualreceiveddrugsphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetIndividualReceivedDrugsPhrPerModel VM = new VMGetIndividualReceivedDrugsPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {
            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }


            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(disease_info->>'drug_id') \r\nFROM health_screening AS hs\r\nCROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS disease_info " + InstitutionParam + " WHERE jsonb_typeof(hs.drugs) = 'array' \r\n  AND (disease_info->>'drug_id') IS NOT NULL\r\n " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Individual_Received_Drug_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetTotalConfirmedMTMBeneficiaryPhrPer")]
        public VMGetTotalConfirmedMTMBeneficiaryPhrPerModel gettotalconfirmedmtmbeneficiaryphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetTotalConfirmedMTMBeneficiaryPhrPerModel VM = new VMGetTotalConfirmedMTMBeneficiaryPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {
            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hh.family_id = fmm.family_id "+ InstitutionParam + "";
            InstitutionParam = InstitutionParams;

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(DISTINCT hh.member_id) FROM health_history as hh " + InstitutionParam + " WHERE hh.mtm_beneficiary->>'avail_service' = 'yes'\r\nOR hh.mtm_beneficiary->>'diabetes_mellitus' = 'yes'\r\nOR hh.mtm_beneficiary->>'dialysis_capd' = 'yes'\r\nOR hh.mtm_beneficiary->>'dialysis_capd' = 'yes'\r\nOR hh.mtm_beneficiary->>'palliative_care' = 'yes'\r\nOR hh.mtm_beneficiary->>'physiotherapy' = 'yes'\r\n " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Total_Confirmed_Beneficiary_Count = drInner["count"].ToString();

            }

            con.Close();

            return VM;

        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetFacilityWiseScreeningPhrPer")]
        public VMGetFacilityWiseScreeningPhrPerModel[] getfacilitywiseScreeningphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetFacilityWiseScreeningPhrPerModel VM = new VMGetFacilityWiseScreeningPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT ftm.facility_type_name,fcm.category_name,COUNT(DISTINCT hs.member_id) AS count_records\r\nFROM public.facility_registry fr\r\nINNER JOIN facility_type_master ftm ON fr.facility_type_id = ftm.facility_type_id\r\nINNER JOIN facility_category_master fcm ON fr.category_id = fcm.category_id\r\nINNER JOIN family_member_master fmm ON fmm.facility_id = fr.facility_id\r\nINNER JOIN health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY ftm.facility_type_name,fcm.category_name order by count_records DESC LIMIT 12";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetFacilityWiseScreeningPhrPerModel> RList = new List<VMGetFacilityWiseScreeningPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetFacilityWiseScreeningPhrPerModel();

                SList.Facility_Type_Name = drInner["facility_type_name"].ToString();
                SList.Category_Name = drInner["category_name"].ToString();
                SList.Facility_Wise_Screening_Count = drInner["count_records"].ToString();

                RList.Add(SList);

            }

            con.Close();

            VMGetFacilityWiseScreeningPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationRDMNDStatusPhrPer")]
        public VMGetPopulationRDMNDStatusPhrPerModel[] getpopulationrdmndstatusphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPopulationRDMNDStatusPhrPerModel VM = new VMGetPopulationRDMNDStatusPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'status'='Resident' " + CommunityParam + "";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetPopulationRDMNDStatusPhrPerModel> RList = new List<VMGetPopulationRDMNDStatusPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetPopulationRDMNDStatusPhrPerModel();

                SList.Resident_Count = drInner["count"].ToString();

                RList.Add(SList);

            }

            con.Close();

            con.Open();

            if(RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'status'= 'Migrant' " + CommunityParam + "";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        RList[i].Migrant_Count = drInner1["count"].ToString();

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

                cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'status'= 'Dead'  or fmm.resident_status_details->>'status'='Death' " + CommunityParam + "";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        RList[i].Death_Count = drInner1["count"].ToString();

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

                cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'status'='Duplicate' " + CommunityParam + "";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        RList[i].Duplicate_Count = drInner1["count"].ToString();

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

                cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'status'= 'Non traceable'  or fmm.resident_status_details->>'status'='Non-traceable'\r\n " + CommunityParam + "";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {
                    for (int i = 0; i < RList.Count; i++)
                    {

                        RList[i].Non_Traceable_Count = drInner1["count"].ToString();

                    }
                }

            }

            con.Close();

            VMGetPopulationRDMNDStatusPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetMTMBeneficiariesPhrPer")]
        public VMGetMTMBeneficiariesPhrPerModel getmtmbeneficiariesphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetMTMBeneficiariesPhrPerModel VM = new VMGetMTMBeneficiariesPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') as confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') as confirmed_hypertension,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') \r\nas confirmed_diabetes_mellitus_hypertension,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' and hs.screening_values->>'dm_screening' = 'Known DM'\r\nAND hs.screening_values->>'dm_screening' = 'Known DM' AND hs.screening_values->>'ht_screening' = 'Known HT' \r\n and ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS Both\r\nFROM family_member_master AS fmm\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n " + InstitutionParam  + " " + CommunityParam + "";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();

            while (drInner.Read())
            {
                VM.Confirmed_Diabetes_Mellitus = drInner["confirmed_diabetes_mellitus"].ToString();
                VM.Confirmed_Hypertension = drInner["confirmed_hypertension"].ToString();
                VM.Confirmed_Diabetes_Mellitus_Hypertension = drInner["confirmed_diabetes_mellitus_hypertension"].ToString();
                VM.Controlled_Diabetes_Mellitus = drInner["controlled_diabetes_mellitus"].ToString();
                VM.Controlled_Hypertension = drInner["controlled_hypertension"].ToString();
                VM.Both = drInner["Both"].ToString();
            }

            con.Close();

            return VM;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPBSConditionScreeningPhrPer")]
        public VMGetPBSConditionScreeningPhrPerModel[] getpbsconditionscreeningphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPBSConditionScreeningPhrPerModel VM = new VMGetPBSConditionScreeningPhrPerModel();

            Filterforall(F);

            string InstitutionParams = "";

            string InstitutionPara = "";

            string CommaParam = "";

            if (CommunityParam != "" && InstitutionParam == "")
            {

                InstitutionParams = " INNER JOIN family_member_master as fmm ON b.member_id = fmm.member_id,";

            }
            else if (CommunityParam != "" && InstitutionParam != "")
            {

                InstitutionParams = " INNER JOIN family_member_master as fmm ON b.member_id = fmm.member_id";

            }
            else
            {
                CommaParam = ",";
            }

            if (InstitutionParam != "")
            {

                InstitutionPara = " INNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id,";

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "WITH disease_ids (id) AS (\r\n    VALUES ('05e261e3-0ebb-422e-a88f-c13438bfa697'::uuid),\r\n           ('0C7FFA38-8511-40EF-BFC3-534C91DFA3FA'::uuid),\r\n\t       ('3525238f-8c2c-4777-9770-4dea9323b601'::uuid),\r\n\t       ('555a1ef5-a30f-4218-b7e8-2c5583f28130'::uuid),\r\n\t       ('7a24431f-f3f2-4285-ac0d-9c5715adcc86'::uuid),\r\n\t       ('a2a0ba96-e0de-47a8-aa32-35faa6cade88'::uuid),\r\n\t       ('ab933497-3fc9-45af-8842-99f8630eaebb'::uuid),\r\n\t       ('c853f5e5-be80-4edb-a688-409a196c07c3'::uuid),\r\n\t       ('3a4a62f0-4664-4266-a998-95253e4a611a'::uuid),\r\n\t       ('4e982bba-ce8e-4179-99a6-ae6c25431e3e'::uuid)\t\r\n)\r\nSELECT\r\n\tdiagnosis_name,\r\n    COUNT(*) AS disease_count\r\nFROM (\r\n    SELECT\r\n        (disease_info->>'id')::uuid AS disease_id\r\n    FROM\r\n        health_screening as b " + CommaParam + " " + InstitutionParams + " " + InstitutionPara + " jsonb_array_elements(b.diseases-> 0 ->'disease_list') AS disease_info,\r\n        disease_ids\r\n    WHERE\r\n        jsonb_typeof(b.diseases-> 0 ->'disease_list') = 'array' and (disease_info->>'id')::uuid NOT IN (select id from disease_ids) " + CommunityParam + " ) subquery LEFT JOIN health_diagnosis_master hd on hd.diagnosis_id = subquery.disease_id\r\nGROUP BY\r\n    hd.diagnosis_name\r\norder by disease_count Desc LIMIT 6";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetPBSConditionScreeningPhrPerModel> RList = new List<VMGetPBSConditionScreeningPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetPBSConditionScreeningPhrPerModel();

                SList.Diagnosis_Name = drInner["diagnosis_name"].ToString();
                SList.Disease_Count = drInner["disease_count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetPBSConditionScreeningPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPopulationVerifiedNonVerifiedStatusPhrPer")]
        public VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel[] getpopulationverifiednonverifiedstatusphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel VM = new VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'resident_details'='Verified'\r\n " + CommunityParam + "";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel> RList = new List<VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel();

                SList.Population_Verified_Count = drInner["count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            con.Open();

            if(RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT COUNT(fmm.member_id) FROM family_member_master as fmm " + InstitutionParam + " WHERE fmm.resident_status_details->>'resident_details'='Unverified'\r\n " + CommunityParam + "";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {
                    for(int i = 0; i<RList.Count; i++)
                    {

                        RList[i].Population_Non_Verified_Count = drInner1["count"].ToString();

                    }
                }

            }

            con.Close();

            VMGetPopulationVerifiedNonVerifiedStatusPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetIndividualScreenedAgeWisePhrPer")]
        public VMGetIndividualScreenedAgeWisePhrPerModel[] getindividualscreenedagewisephrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetIndividualScreenedAgeWisePhrPerModel VM = new VMGetIndividualScreenedAgeWisePhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COUNT(fmm.member_id),\r\n  CASE\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 0 AND 17 THEN 'Below 10'\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 18 AND 19 THEN '10 to 19'\r\n    WHEN date_part('year', age(birth_date)) BETWEEN 20 AND 29 THEN '20 to 29'\r\n\tWHEN date_part('year', age(birth_date)) BETWEEN 30 AND 39 THEN '30 to 39'\r\n\tWHEN date_part('year', age(birth_date)) BETWEEN 40 AND 49 THEN '40 to 49'\r\n\tWHEN date_part('year', age(birth_date)) BETWEEN 50 AND 59 THEN '50 to 59'\r\n\tWHEN date_part('year', age(birth_date)) BETWEEN 60 AND 69 THEN '60 to 69'\r\n\tWHEN date_part('year', age(birth_date)) BETWEEN 70 AND 79 THEN '70 to 79'\r\n\tWHEN date_part('year', age(birth_date)) >=80 THEN '80 or Above'\r\n  END AS age_group\r\nfrom family_member_master fmm inner join health_screening hs ON fmm.member_id = hs.member_id\r\ninner join health_history hh ON hs.member_id = hh.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY age_group";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetIndividualScreenedAgeWisePhrPerModel> RList = new List<VMGetIndividualScreenedAgeWisePhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetIndividualScreenedAgeWisePhrPerModel();

                SList.Age_Group = drInner["age_group"].ToString();
                SList.Age_Count = drInner["count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetIndividualScreenedAgeWisePhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetLabTestPhrPer")]
        public VMGetLabTestPhrPerModel[] getlabtestphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetLabTestPhrPerModel VM = new VMGetLabTestPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id "+InstitutionParam+"";
            InstitutionParam = InstitutionParams;

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT (lab_info->>'test_name') as labTestName, COUNT(*) AS lab_test_count\r\nFROM health_screening as hs " + InstitutionParam + " CROSS JOIN LATERAL jsonb_array_elements(hs.lab_test) AS lab_info\r\nWHERE jsonb_typeof(hs.lab_test) = 'array' and (lab_info->>'test_id')::uuid IN (SELECT lab_test_id FROM health_lab_tests_master) " + CommunityParam + " GROUP BY labTestName order by lab_test_count Desc";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetLabTestPhrPerModel> RList = new List<VMGetLabTestPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetLabTestPhrPerModel();

                SList.Lab_Test_Name = drInner["labTestName"].ToString();
                SList.Lab_Test_Count = drInner["lab_test_count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetLabTestPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetScreenedVillageTypeWisePhrPer")]
        public VMGetScreenedVillageTypeWisePhrPerModel[] getscreenedvillagetypewisephrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetScreenedVillageTypeWisePhrPerModel VM = new VMGetScreenedVillageTypeWisePhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT COALESCE(avm.village_type, 'Other') AS village_type, COUNT(hs.screening_id) FROM address_village_master avm inner join family_member_master fmm ON avm.village_id = fmm.village_id\r\ninner join health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY village_type";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetScreenedVillageTypeWisePhrPerModel> RList = new List<VMGetScreenedVillageTypeWisePhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetScreenedVillageTypeWisePhrPerModel();

                SList.Village_Type = drInner["village_type"].ToString();
                SList.Village_Type_Count = drInner["count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetScreenedVillageTypeWisePhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetReferredSplitUpPhrPer")]
        public VMGetReferredSplitUpPhrPerModel[] getreferredsplitupphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetReferredSplitUpPhrPerModel VM = new VMGetReferredSplitUpPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT fcm.category_name, COUNT(fmm.member_id) as referredcount \r\nFROM facility_registry fr \r\ninner join facility_category_master fcm ON fr.category_id = fcm.category_id \r\ninner join family_member_master fmm ON fr.facility_id = fmm.facility_id\r\ninner join health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " WHERE hs.outcome->'cancer_breast'->>'referral_type' <> '' OR \r\n\ths.outcome->'cancer_cervical'->>'referral_type' <> '' OR\r\n\ths.outcome->'cancer_oral'->>'referral_type' <> '' OR\r\n\ths.outcome->'ckd'->>'referral_type' <> '' OR\r\n\ths.outcome->'copd'->>'referral_type' <> '' OR\r\n\ths.outcome->'covid_19'->>'referral_type' <> '' OR\r\n\ths.outcome->'diabetes'->>'referral_type' <> '' OR\r\n\ths.outcome->'hypertension'->>'referral_type' <> '' OR\r\n\ths.outcome->'leprosy'->>'referral_type' <> '' OR\r\n\ths.outcome->'mental_health'->>'referral_type' <> '' " + CommunityParam + " GROUP BY fcm.category_name ORDER BY referredcount DESC";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetReferredSplitUpPhrPerModel> RList = new List<VMGetReferredSplitUpPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetReferredSplitUpPhrPerModel();

                SList.Category_Name = drInner["category_name"].ToString();
                SList.Referred_Count = drInner["referredcount"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetReferredSplitUpPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetUHCConditionScreeningPhrPer")]
        public VMGetUHCConditionScreeningPhrPerModel[] getuhcconditionscreeningphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetUHCConditionScreeningPhrPerModel VM = new VMGetUHCConditionScreeningPhrPerModel();

            Filterforall(F);

            string InstitutionParams = "";

            string InstitutionPara = "";

            string CommaParam = "";

            if (CommunityParam != "" && InstitutionParam == "")
            {

                InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id,";

            }
            else if (CommunityParam != "" && InstitutionParam != "")
            {

               InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id";

            }
            else
            {
                CommaParam = ",";
            }

            if(InstitutionParam != "")
            {

                InstitutionPara = " INNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id,";

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT hdm.service_name,count(*)\r\nFROM health_screening as hs " + CommaParam + " " + InstitutionParams + " " + InstitutionPara + " jsonb_array_elements(hs.diseases-> 0 ->'disease_list') AS disease_info\r\nINNER JOIN health_diagnosis_master as hdm ON (disease_info->>'id')::uuid = hdm.diagnosis_id WHERE jsonb_typeof(hs.diseases-> 0 ->'disease_list') = 'array' " + CommunityParam + " group by hdm.service_name";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetUHCConditionScreeningPhrPerModel> RList = new List<VMGetUHCConditionScreeningPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetUHCConditionScreeningPhrPerModel();

                SList.Service_Name = drInner["service_name"].ToString();
                SList.Total_Screening_Count = drInner["count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetUHCConditionScreeningPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrugsIssuedPhrPer")]
        public VMGetDrugsIssuedPhrPerModel[] getdrugsissuedphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetDrugsIssuedPhrPerModel VM = new VMGetDrugsIssuedPhrPerModel();

            Filterforall(F);

            if(CommunityParam != "")
            {

            string InstitutionParams = " INNER JOIN family_member_master as fmm ON hs.family_id = fmm.family_id "+InstitutionParam+"";
            InstitutionParam = InstitutionParams;

            }

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT (drug_info->>'drug_name') as drugName, COUNT(*) AS drug_count\r\nFROM health_screening as hs " + InstitutionParam + " CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\nWHERE jsonb_typeof(hs.drugs) = 'array' and (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master) " + CommunityParam + " GROUP BY drugName order by drug_count Desc";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetDrugsIssuedPhrPerModel> RList = new List<VMGetDrugsIssuedPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetDrugsIssuedPhrPerModel();

                SList.Drug_Name = drInner["drugName"].ToString();
                SList.Drug_Count = drInner["drug_count"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetDrugsIssuedPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetRoleBasedScreeningPhrPer")]
        public VMGetRoleBasedScreeningPhrPerModel[] getrolebasedscreeningphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetRoleBasedScreeningPhrPerModel VM = new VMGetRoleBasedScreeningPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmdInner = new NpgsqlCommand();
            cmdInner.Connection = con;
            cmdInner.CommandType = CommandType.Text;

            cmdInner.CommandText = "SELECT role_name, count(hs.screening_id) as screeningcount FROM user_role_master urm inner join user_master um ON urm.role_id = um.role\r\ninner join family_member_master fmm ON um.facility_id = fmm.facility_id \r\ninner join health_screening hs ON fmm.member_id = hs.member_id " + InstitutionParam + " " + CommunityParam + " GROUP BY role_name ORDER BY screeningcount DESC";

            NpgsqlDataReader drInner = cmdInner.ExecuteReader();
            List<VMGetRoleBasedScreeningPhrPerModel> RList = new List<VMGetRoleBasedScreeningPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetRoleBasedScreeningPhrPerModel();

                SList.Role_Name = drInner["role_name"].ToString();
                SList.Total_Screening_Count = drInner["screeningcount"].ToString();

                RList.Add(SList);
            }

            con.Close();

            VMGetRoleBasedScreeningPhrPerModel[] RArray = RList.ToArray();

            return RArray;

        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPHRHealthPhrPer")]
        public VMGetPHRHealthPhrPerModel[] getphrhealthphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPHRHealthPhrPerModel VM = new VMGetPHRHealthPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n       CASE WHEN fr.facility_level = 'PHC' THEN ftm.facility_type_name END AS phc,\r\n       CASE WHEN fr.facility_level = 'HSC' THEN ftm.facility_type_name END AS hsc\r\nFROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN facility_registry AS fr ON fmm.facility_id = fr.facility_id\r\nINNER JOIN facility_type_master AS ftm ON fr.facility_type_id = ftm.facility_type_id WHERE fr.facility_level IN ('PHC', 'HSC') " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid, fr.facility_level,ftm.facility_type_name LIMIT 4999";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetPHRHealthPhrPerModel> RList = new List<VMGetPHRHealthPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetPHRHealthPhrPerModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Village_Name = drInner["village_name"].ToString();
                SList.Village_Gid = drInner["village_gid"].ToString();
                SList.PHC = drInner["phc"].ToString();
                SList.HSC = drInner["hsc"].ToString();


                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nCOUNT(DISTINCT hs.member_id) FROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Unique_Screening = drInner1["count"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nCOUNT(hs.member_id) FROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nCOUNT(DISTINCT hs.member_id) FROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\nwhere date_part('year',age(fmm.birth_date))>'18'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Unique_Screening_Count = drInner1["count"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nCOUNT(fmm.member_id) FROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nwhere date_part('year',age(fmm.birth_date))>'18'\r\n" +  CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE fmm.resident_status_details->>'resident_details'='Verified') AS verified_population,\r\ncount(*) FILTER (WHERE fmm.resident_status_details->>'status'= 'Dead'  or fmm.resident_status_details->>'status'='Death') AS death_count,\r\ncount(*) FILTER (WHERE fmm.resident_status_details->>'status'='Resident') AS resident_count,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE fmm.resident_status_details->>'status'='Resident') / \r\nNULLIF(count(*) FILTER (WHERE fmm.resident_status_details->>'resident_details'='Verified'), 0) * 100,0) AS resident_verified_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {
                            RList[i].Verified_Population = drInner1["verified_population"].ToString();
                            RList[i].Death_Count = drInner1["death_count"].ToString();
                            RList[i].Resident_Count = drInner1["resident_count"].ToString();
                            RList[i].Resident_Verified_Percentage = drInner1["resident_verified_percentage"].ToString();

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

                cmdInner.CommandText = "SELECT district_name, district_gid, hud_name, hud_gid, block_name, block_gid,village_name,village_gid,\r\nROUND((screening18 * 100.0) / total_screening, 2) AS percentagescreening18\r\nFROM (\r\n  SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid,avm.village_name,avm.village_gid,\r\n         COUNT(*) AS total_screening,\r\n         COUNT(*) FILTER (WHERE date_part('year', age(fmm.birth_date)) >= 18) AS screening18\r\n  FROM address_block_master AS abm\r\n  INNER JOIN family_member_master AS fmm ON abm.block_id = fmm.block_id\r\n  INNER JOIN health_screening as hs ON fmm.member_id = hs.member_id\r\n  INNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\n  INNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\n  INNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid,avm.village_name,avm.village_gid\r\n) AS merged_data LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Percentage_Screening = drInner1["percentagescreening18"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM'), 0) * 100,0) AS diabetes_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0) AS hypertension_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\nCOALESCE(\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') / \r\nNULLIF(count(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT'), 0) * 100,0)\r\nAS both_percentage\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

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

            VMGetPHRHealthPhrPerModel[] RArray = RList.ToArray();

            return RArray;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetPHRMTMPhrPer")]
        public VMGetPHRMTMPhrPerModel[] getphrmtmphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetPHRMTMPhrPerModel VM = new VMGetPHRMTMPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nftm.facility_type_name as userfacilitytype,CASE WHEN fr.facility_level = 'PHC' THEN ftm.facility_type_name END AS phc,\r\n       CASE WHEN fr.facility_level = 'HSC' THEN ftm.facility_type_name END AS hsc\r\nFROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN facility_registry AS fr ON fmm.facility_id = fr.facility_id\r\nINNER JOIN facility_type_master AS ftm ON fr.facility_type_id = ftm.facility_type_id\r\nWHERE fr.facility_level IN ('PHC', 'HSC') " + CommunityParam  + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid, fr.facility_level, ftm.facility_type_name LIMIT 4999";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetPHRMTMPhrPerModel> RList = new List<VMGetPHRMTMPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetPHRMTMPhrPerModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Village_Name = drInner["village_name"].ToString();
                SList.Village_Gid = drInner["village_gid"].ToString();
                SList.User_Facility_Type = drInner["userfacilitytype"].ToString();
                SList.PHC = drInner["phc"].ToString();
                SList.HSC = drInner["hsc"].ToString();


                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'palliative_care' <> '') as palliativecaremtmupdated,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'physiotherapy' <> '') as physiotherapymtmupdated,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'dialysis_capd' <> '') as capdmtmupdated,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'palliative_care' <> '') +\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'physiotherapy' <> '') +\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary->>'dialysis_capd' <> '') as bothmtmupdated \r\nFROM family_member_master AS fmm\r\nINNER JOIN health_history AS hh ON fmm.member_id = hh.member_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Palliative_Care_Mtm_Updated = drInner1["palliativecaremtmupdated"].ToString();
                            RList[i].Physiotherapy_Mtm_Updated = drInner1["physiotherapymtmupdated"].ToString();
                            RList[i].Capd_Mtm_Updated = drInner1["capdmtmupdated"].ToString();
                            RList[i].Both_Mtm_Updated = drInner1["bothmtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM') AS confirmed_diabetes_mellitus,\r\ncount(*) FILTER (WHERE hs.screening_values->>'dm_screening' = 'Known DM' and hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'ht_screening' = 'Known HT') AS confirmed_hypertension\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Confirmed_Diabetes_Mellitus = drInner1["confirmed_diabetes_mellitus"].ToString();
                            RList[i].Confirmed_Both = drInner1["confirmed_both"].ToString();
                            RList[i].Confirmed_Hypertension = drInner1["confirmed_hypertension"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM' and \r\n((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_both,\r\ncount(*) FILTER (WHERE hs.screening_values->>'rbs' < '140' AND hs.screening_values->>'dm_screening' = 'Known DM') AS controlled_diabetes_mellitus,\r\ncount(*) FILTER (WHERE ((screening_values->>'dia_bp')::numeric < 90 OR (screening_values->>'sys_bp')::numeric < 140) \r\nAND hs.screening_values->>'ht_screening' = 'Known HT') AS controlled_hypertension\r\nFROM address_village_master as avm\r\nINNER JOIN family_member_master as fmm ON avm.village_id = fmm.village_id\r\nINNER JOIN address_district_master as adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master as ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master as abm ON fmm.block_id = abm.block_id\r\nINNER JOIN health_screening as hs ON fmm.member_id = hs.member_id " + CommunityParam  + " GROUP BY adm.district_name,adm.district_gid,ahm.hud_name,ahm.hud_gid,abm.block_name,abm.block_gid,avm.village_name,avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Controlled_Both = drInner1["controlled_both"].ToString();
                            RList[i].Controlled_Diabetes_Mellitus = drInner1["controlled_diabetes_mellitus"].ToString();
                            RList[i].Controlled_Hypertension = drInner1["controlled_hypertension"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetPHRMTMPhrPerModel[] RArray = RList.ToArray();

            return RArray;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetStaffHealthPhrPer")]
        public VMGetStaffHealthPhrPerModel[] getstaffhealthphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetStaffHealthPhrPerModel VM = new VMGetStaffHealthPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n       CASE WHEN fr.facility_level = 'PHC' THEN ftm.facility_type_name END AS phc,\r\n       CASE WHEN fr.facility_level = 'HSC' THEN ftm.facility_type_name END AS hsc\r\nFROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN facility_registry AS fr ON fmm.facility_id = fr.facility_id\r\nINNER JOIN facility_type_master AS ftm ON fr.facility_type_id = ftm.facility_type_id\r\nWHERE fr.facility_level IN ('PHC', 'HSC') " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid, fr.facility_level, ftm.facility_type_name LIMIT 4999";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetStaffHealthPhrPerModel> RList = new List<VMGetStaffHealthPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetStaffHealthPhrPerModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Village_Name = drInner["village_name"].ToString();
                SList.Village_Gid = drInner["village_gid"].ToString();
                SList.phc = drInner["phc"].ToString();
                SList.hsc = drInner["hsc"].ToString();


                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n\tCOUNT(DISTINCT hs.member_id) AS individual_screening,\r\n    COUNT(hs.member_id) AS total_screening,\r\n    COUNT(CASE WHEN individual_screening.drug_id IS NOT NULL THEN 1 ELSE NULL END) AS individualissueddrug,\r\n    COUNT(CASE WHEN fmm.resident_status_details->>'resident_details' = 'Verified' THEN 1 ELSE NULL END) AS population_verified\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\nLEFT JOIN (\r\n    SELECT fmm.member_id, (drug_info->>'drug_id')::uuid AS drug_id\r\n    FROM family_member_master AS fmm\r\n    INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n    CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\n    WHERE jsonb_typeof(hs.drugs) = 'array'\r\n        AND (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master)\r\n) AS individual_screening ON fmm.member_id = individual_screening.member_id\r\nWHERE um.role = '1b58cec2-2553-4d95-9ac0-9bf194614e9e'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].mlhp_Individual_Screening = drInner1["individual_screening"].ToString();
                            RList[i].mlhp_Total_Screening = drInner1["total_screening"].ToString();
                            RList[i].mlhp_Individual_Drugs = drInner1["individualissueddrug"].ToString();
                            RList[i].mlhp_Population_Verified = drInner1["population_verified"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n\tCOUNT(DISTINCT hs.member_id) AS individual_screening,\r\n    COUNT(hs.member_id) AS total_screening,\r\n    COUNT(CASE WHEN individual_screening.drug_id IS NOT NULL THEN 1 ELSE NULL END) AS individualissueddrug,\r\n    COUNT(CASE WHEN fmm.resident_status_details->>'resident_details' = 'Verified' THEN 1 ELSE NULL END) AS population_verified\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\nLEFT JOIN (\r\n    SELECT fmm.member_id, (drug_info->>'drug_id')::uuid AS drug_id\r\n    FROM family_member_master AS fmm\r\n    INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n    CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\n    WHERE jsonb_typeof(hs.drugs) = 'array'\r\n        AND (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master)\r\n) AS individual_screening ON fmm.member_id = individual_screening.member_id\r\nWHERE um.role = '9bd9c87c-dcde-418d-b11e-4ac62838760f'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].whv_Population_Verified = drInner1["population_verified"].ToString();
                            RList[i].whv_Total_Screening = drInner1["total_screening"].ToString();
                            RList[i].whv_Individual_Screened = drInner1["individual_screening"].ToString();
                            RList[i].whv_Individual_Drugs = drInner1["individualissueddrug"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n\tCOUNT(DISTINCT hs.member_id) AS individual_screening,\r\n    COUNT(hs.member_id) AS total_screening,\r\n    COUNT(CASE WHEN individual_screening.drug_id IS NOT NULL THEN 1 ELSE NULL END) AS individualissueddrug,\r\n    COUNT(CASE WHEN fmm.resident_status_details->>'resident_details' = 'Verified' THEN 1 ELSE NULL END) AS population_verified\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\nLEFT JOIN (\r\n    SELECT fmm.member_id, (drug_info->>'drug_id')::uuid AS drug_id\r\n    FROM family_member_master AS fmm\r\n    INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n    CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\n    WHERE jsonb_typeof(hs.drugs) = 'array'\r\n        AND (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master)\r\n) AS individual_screening ON fmm.member_id = individual_screening.member_id\r\nWHERE um.role = 'e737b5b2-f7a5-4d14-9340-706bb49b7e2a'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dph_Population_Verified = drInner1["population_verified"].ToString();
                            RList[i].dph_Total_Screening = drInner1["total_screening"].ToString();
                            RList[i].dph_Individual_Screening = drInner1["individual_screening"].ToString();
                            RList[i].dph_Individual_Drugs = drInner1["individualissueddrug"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n\tCOUNT(DISTINCT hs.member_id) AS individual_screening,\r\n    COUNT(hs.member_id) AS total_screening,\r\n    COUNT(CASE WHEN individual_screening.drug_id IS NOT NULL THEN 1 ELSE NULL END) AS individualissueddrug,\r\n    COUNT(CASE WHEN fmm.resident_status_details->>'resident_details' = 'Verified' THEN 1 ELSE NULL END) AS population_verified\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\nLEFT JOIN (\r\n    SELECT fmm.member_id, (drug_info->>'drug_id')::uuid AS drug_id\r\n    FROM family_member_master AS fmm\r\n    INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n    CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\n    WHERE jsonb_typeof(hs.drugs) = 'array'\r\n        AND (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master)\r\n) AS individual_screening ON fmm.member_id = individual_screening.member_id\r\nWHERE um.role = 'a679fa29-90c9-414a-9489-45b4ceb96072'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dms_Population_Verified = drInner1["population_verified"].ToString();
                            RList[i].dms_Total_Screening = drInner1["total_screening"].ToString();
                            RList[i].dms_Individual_Drugs = drInner1["individualissueddrug"].ToString();
                            RList[i].dms_Individual_Screening = drInner1["individual_screening"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\n\tCOUNT(DISTINCT hs.member_id) AS individual_screening,\r\n    COUNT(hs.member_id) AS total_screening,\r\n    COUNT(CASE WHEN individual_screening.drug_id IS NOT NULL THEN 1 ELSE NULL END) AS individualissueddrug,\r\n    COUNT(CASE WHEN fmm.resident_status_details->>'resident_details' = 'Verified' THEN 1 ELSE NULL END) AS population_verified\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\nLEFT JOIN (\r\n    SELECT fmm.member_id, (drug_info->>'drug_id')::uuid AS drug_id\r\n    FROM family_member_master AS fmm\r\n    INNER JOIN health_screening AS hs ON fmm.member_id = hs.member_id\r\n    CROSS JOIN LATERAL jsonb_array_elements(hs.drugs) AS drug_info\r\n    WHERE jsonb_typeof(hs.drugs) = 'array'\r\n        AND (drug_info->>'drug_id')::uuid IN (SELECT drug_id FROM health_drugs_master)\r\n) AS individual_screening ON fmm.member_id = individual_screening.member_id\r\nWHERE um.role = 'ead9610b-9ce3-418d-b747-747cdba3655e'\r\n" + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dme_Total_Screening = drInner1["total_screening"].ToString();
                            RList[i].dme_Individual_Drugs = drInner1["individualissueddrug"].ToString();
                            RList[i].dme_Individual_Screening = drInner1["individual_screening"].ToString();
                            RList[i].dme_Population_Verified = drInner1["population_verified"].ToString();
                        }
                    }

                }

            }

            con.Close();


            VMGetStaffHealthPhrPerModel[] RArray = RList.ToArray();

            return RArray;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetStaffMTMPhrPer")]
        public VMGetStaffMTMPhrPerModel[] getstaffmtmphrper([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMGetStaffMTMPhrPerModel VM = new VMGetStaffMTMPhrPerModel();

            Filterforall(F);

            con.Open();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            cmd.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\nftm.facility_type_name as userfacilitytype,\r\n       CASE WHEN fr.facility_level = 'PHC' THEN ftm.facility_type_name END AS phc,\r\n       CASE WHEN fr.facility_level = 'HSC' THEN ftm.facility_type_name END AS hsc\r\nFROM family_member_master AS fmm\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN facility_registry AS fr ON fmm.facility_id = fr.facility_id\r\nINNER JOIN facility_type_master AS ftm ON fr.facility_type_id = ftm.facility_type_id\r\nWHERE fr.facility_level IN ('PHC', 'HSC') " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid, fr.facility_level, ftm.facility_type_name LIMIT 4999";

            NpgsqlDataReader drInner = cmd.ExecuteReader();
            List<VMGetStaffMTMPhrPerModel> RList = new List<VMGetStaffMTMPhrPerModel>();

            while (drInner.Read())
            {
                var SList = new VMGetStaffMTMPhrPerModel();

                SList.District_Name = drInner["district_name"].ToString();
                SList.District_Gid = drInner["district_gid"].ToString();
                SList.Hud_Name = drInner["hud_name"].ToString();
                SList.Hud_Gid = drInner["hud_gid"].ToString();
                SList.Block_Name = drInner["block_name"].ToString();
                SList.Block_Gid = drInner["block_gid"].ToString();
                SList.Village_Name = drInner["village_name"].ToString();
                SList.Village_Gid = drInner["village_gid"].ToString();
                SList.User_Facility_Type = drInner["userfacilitytype"].ToString();
                SList.phc = drInner["phc"].ToString();
                SList.hsc = drInner["hsc"].ToString();


                RList.Add(SList);

            }
            con.Close();

            con.Open();


            if (RList.Count > 0)
            {

                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as dphmtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id WHERE um.role = 'e737b5b2-f7a5-4d14-9340-706bb49b7e2a' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dph_Mtm_Updated = drInner1["dphmtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as dmsmtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id WHERE um.role = 'a679fa29-90c9-414a-9489-45b4ceb96072' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dms_Mtm_Updated = drInner1["dmsmtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as dmemtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id WHERE um.role = 'ead9610b-9ce3-418d-b747-747cdba3655e' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].dme_Mtm_Updated = drInner1["dmemtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as palliativecaremtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id\r\n WHERE um.role = '0e71ef72-3dda-424d-b03a-6a538e1f16c8' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].Palliative_Mtm_Updated = drInner1["palliativecaremtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as whvmtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id\r\n WHERE um.role = '9bd9c87c-dcde-418d-b11e-4ac62838760f' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].whv_Mtm_Updated = drInner1["whvmtmupdated"].ToString();
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

                cmdInner.CommandText = "SELECT adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid,\r\ncount(*)FILTER (WHERE hh.mtm_beneficiary is not null) as mlhpmtmupdated\r\nFROM family_member_master AS fmm\r\nINNER JOIN user_master um ON fmm.facility_id = um.facility_id\r\nINNER JOIN address_district_master AS adm ON fmm.district_id = adm.district_id\r\nINNER JOIN address_hud_master AS ahm ON fmm.hud_id = ahm.hud_id\r\nINNER JOIN address_block_master AS abm ON fmm.block_id = abm.block_id\r\nINNER JOIN address_village_master AS avm ON fmm.village_id = avm.village_id\r\nINNER JOIN health_history as hh ON fmm.member_id = hh.member_id\r\n WHERE um.role = '1b58cec2-2553-4d95-9ac0-9bf194614e9e' " + CommunityParam + " GROUP BY adm.district_name, adm.district_gid, ahm.hud_name, ahm.hud_gid, abm.block_name, abm.block_gid, avm.village_name, avm.village_gid LIMIT 4999";

                NpgsqlDataReader drInner1 = cmdInner.ExecuteReader();

                while (drInner1.Read())
                {

                    for (int i = 0; i < RList.Count; i++)
                    {
                        if (RList[i].Village_Gid == drInner1["village_gid"].ToString())
                        {

                            RList[i].mlhp_Mtm_Updated = drInner1["mlhpmtmupdated"].ToString();
                        }
                    }

                }

            }

            con.Close();

            VMGetStaffMTMPhrPerModel[] RArray = RList.ToArray();

            return RArray;
        }
    }
}

