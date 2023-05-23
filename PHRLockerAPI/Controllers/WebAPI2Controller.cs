//using Dapper;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
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
    //[Route("api/[controller]")]
    [ApiController]
    public class WebAPI2Controller : ControllerBase
    {
        private readonly Ismsgateway _ismsgateway;
        private readonly IConfiguration _configuration;

        private readonly DapperContext context;

        public WebAPI2Controller(DapperContext context, IConfiguration configuration)
        {
            this.context = context;
            _configuration = configuration;
        }

        string CommunityParam = "";
        string InstitutionParam = "";

        //public WebAPI2Controller(IConfiguration configuration, Ismsgateway ismsgateway)
        //{
        //    _configuration = configuration;
        //    _ismsgateway = ismsgateway;
        //}


        [HttpGet]
        [Route("FilterAlldapper")]
        public void Filterforalldapper(FilterpayloadModel F)
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
                            Disparam = Disparam + "(fm.district_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(fm.district_id = '" + v + "') or";
                        }
                        i++;
                    }

                    Disparam = "and" + Disparam;

                }
                else
                {
                    Disparam = "and (fm.district_id = ''" + F.district_id + "'')";
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
                            Disparam = Disparam + "(FR.district_id = ''" + v + "'')";
                        }
                        else
                        {
                            Disparam = Disparam + "(FR.district_id = ''" + v + "'') or";
                        }

                        i++;
                    }

                    Disparam = "and " + Disparam;

                }
                else
                {
                    Disparam = "and (FR.district_id = ''" + F.indistrict_id + "'')";
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

        [HttpGet]
        [Route("FilterAlls")]
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

                    Disparam = "and" + Disparam;

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



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrugdistricttest")]
        public async Task<IEnumerable<OdrugdistrictModel>> GetDrugdistricttest([FromQuery] FilterpayloadModel F)
        {

            Filterforall(F);

            //var query = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            var query = "SELECT * from public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";

            using (var connection = context.CreateConnection())
            {
                var OBJ = await connection.QueryAsync<OdrugdistrictModel>(query);
                return OBJ.ToList();
            }
        }




        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrugdistrict")]
        public getDrugListModelList getdistrict([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            //VMCommunityTriage VM = new VMCommunityTriage();
            getDrugListModelList VM = new getDrugListModelList();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(S.member_id) TotalCount from  public.health_screening as S inner join public.family_master as M on M.family_id=S.family_id inner join public.address_district_master as MS on M.district_id=MS.district_id where s.drugs!='null' group by MS.district_name,MS.district_gid";


            Filterforall(F);

            cmd.CommandText = String.Format("select * from public.getdrugdistrict('"+ CommunityParam + "','"+ InstitutionParam + "')");

            //cmd.CommandText = "SELECT public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<GetDrugdistrictModel> RList = new List<GetDrugdistrictModel>();

            while (dr.Read())
            {

                var SList = new GetDrugdistrictModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();
            VM.DistrictWise = RList;

            return VM;
        }


        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrugBlock")]
        public List<BlockModel> getBlock([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "select BL.block_name,BL.block_gid,count(S.member_id) TotalCount from  public.health_screening as S inner join public.family_master as M on M.family_id=S.family_id inner join public.address_block_master as BL on BL.block_id = M.block_id where s.drugs!='null' group by BL.block_name,BL.block_gid";



            cmd.CommandText = "select BL.block_name,BL.block_gid,HD.hud_gid,HD.hud_name,count(screening_id) TotalCount from (SELECT(B.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER,screening_id, family_id  FROM PUBLIC.HEALTH_SCREENING B WHERE drugs != 'null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER, screening_id, member_id) tbl inner join family_master fm on fm.family_id = tbl.family_id  " + CommunityParam + " inner join address_district_master adm on adm.district_id = fm.district_id INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = CAST(UM.USER_ID as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "inner join public.address_block_master as BL on BL.block_id = fm.block_id inner join address_hud_master HD on HD.hud_id=BL.Hud_id group by BL.block_name, BL.block_gid, HD.hud_gid, HD.hud_name";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<BlockModel> RList = new List<BlockModel>();

            while (dr.Read())
            {

                var SList = new BlockModel();

                SList.block_name = dr["block_name"].ToString();
                SList.block_gid = dr["block_gid"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();


            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetmtmbenBlock")]
        public List<BlockModel> GetmtmbenBlock([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));


            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;

            //cmd.CommandText = "select BL.block_name,BL.block_gid,count(S.member_id) TotalCount from  public.health_screening as S inner join public.family_master as M on M.family_id=S.family_id inner join public.address_block_master as BL on BL.block_id = M.block_id where s.drugs!='null' group by BL.block_name,BL.block_gid";



            cmd.CommandText = "select tbl.block_name,tbl.block_gid,tbl.hud_gid,tbl.hud_name,sum(TotalCount) TotalCount from (SELECT(B.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER, BL.block_name, BL.block_gid, HD.hud_gid, HD.hud_name, count(medical_history_id) TotalCount FROM  PUBLIC.HEALTH_history B  inner join family_master fm on fm.family_id = b.family_id " + CommunityParam + "  inner join address_district_master adm on adm.district_id = fm.district_id  inner join public.address_block_master as BL on BL.block_id = fm.block_id  inner join address_hud_master HD on HD.hud_id=BL.Hud_id WHERE b.mtm_beneficiary->>'avail_service'='yes'  and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array'  GROUP BY ARRUSER,BL.block_name,BL.block_gid,HD.hud_gid,HD.hud_name) tbl  INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = CAST(UM.USER_ID as text)  INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + " group by tbl.block_name, tbl.block_gid, tbl.hud_gid, tbl.hud_name";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<BlockModel> RList = new List<BlockModel>();

            while (dr.Read())
            {

                var SList = new BlockModel();

                SList.block_name = dr["block_name"].ToString();
                SList.block_gid = dr["block_gid"].ToString();
                SList.hud_gid = dr["hud_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();


            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrugVillage")]
        public List<VillageModel> getVillage([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));

            Filterforall(F);

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select VL.village_name,VL.village_gid,count(S.member_id) TotalCount from  public.health_screening as S inner join public.family_master as M on M.family_id=S.family_id inner join public.address_village_master as VL on VL.village_id = M.village_id where s.drugs!='null' group by VL.village_name,VL.village_gid";


            cmd.CommandText = "select VL.village_name,VL.village_gid,BL.block_name,BL.block_gid,HD.hud_gid,HD.hud_name,adm.district_gid\r\n,adm.district_name,count(screening_id) TotalCount from (SELECT (B.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER, \r\nscreening_id,family_id  FROM PUBLIC.HEALTH_SCREENING B WHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' \r\nGROUP BY ARRUSER,screening_id,member_id) tbl inner join family_master fm on fm.family_id=tbl.family_id   " + CommunityParam + "\r\nINNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = CAST(UM.USER_ID  as text) INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID   " + InstitutionParam + "\r\ninner join address_district_master adm on adm.district_id=fm.district_id inner join public.address_village_master as VL on VL.village_id = fm.village_id inner join public.address_block_master as BL on BL.block_id = fm.block_id inner join address_hud_master HD on HD.hud_id=BL.Hud_id group by VL.village_name,VL.village_gid,BL.block_name,BL.block_gid,HD.hud_gid,HD.hud_name\r\n,adm.district_gid,adm.district_name";


            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<VillageModel> RList = new List<VillageModel>();

            while (dr.Read())
            {

                var SList = new VillageModel();

                SList.village_name = dr["village_name"].ToString();
                SList.village_gid = dr["village_gid"].ToString();

                SList.district_gid = dr["district_gid"].ToString();
                SList.district_name = dr["district_name"].ToString();

                SList.hud_gid = dr["hud_gid"].ToString();
                SList.hud_name = dr["hud_name"].ToString();

                SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();


            return RList;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetDrughud")]
        public VMCommunityTriage gethud([FromQuery] FilterpayloadModel F)
        {



            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            Filterforall(F);

            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            //cmdHud.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(S.member_id) TotalCount from  public.health_screening as S  inner join public.family_Master as M on M.family_id=S.family_id inner join public.address_district_master as MS on MS.district_id=M.district_id  inner join public.address_hud_master as hu on hu.HUD_id=M.HUD_id  where  s.drugs!='null' group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid";


            cmdHud.CommandText = "select hu.hud_gid,hu.HUD_name,adm.district_gid,adm.district_name,count(screening_id) TotalCount from (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\n screening_id,family_id  FROM PUBLIC.HEALTH_SCREENING B\r\n WHERE drugs!='null' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,screening_id,member_id) tbl\r\n inner join family_master fm on fm.family_id=tbl.family_id \r\n " + CommunityParam + "\r\n inner join address_district_master adm on adm.district_id=fm.district_id\r\n INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = CAST(UM.USER_ID  as text)  \r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID inner join public.address_hud_master as hu on hu.HUD_id=fm.HUD_id  \r\n " + InstitutionParam + "  group by hu.hud_gid,hu.HUD_name,adm.district_name,adm.district_gid";


            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<HudModel> RListHud = new List<HudModel>();
            while (drHud.Read())
            {
                var SList = new HudModel();
                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                //SList.hud_id = drHud["hud_id"].ToString();
                SList.district_gid = drHud["district_gid"].ToString();
                SList.district_name = drHud["district_name"].ToString();
                SList.TotalCount = drHud["TotalCount"].ToString();
                RListHud.Add(SList);
            }
            con.Close();
            VM.HudWise = RListHud;
            return VM;
        }
        [HttpGet]
        [Route("GetDistrictScreenAgeWise")]
        public VMCommunityTriage getdistrictscreening()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid from public.address_district_master as MS order by district_name";
            con.Open();
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
                cmdInner.CommandText = "select MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END AgeText,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id inner join public.health_screening as S  on M.member_id=S.member_id and s.drugs!='null'  group by MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END";

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
                            RList[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].above30 = drInner["TotalCount"].ToString();
                        }

                    }
                }

            }


            con.Close();
            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [Route("GetHUDScreenAgeWise")]
        public VMCommunityTriage gethudscreening()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select hud_id,hud_name,hud_gid from public.address_hud_master order by hud_name";

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
                cmdInner.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END AgeText from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id inner join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id inner join public.health_screening as S  on M.member_id=S.member_id and s.drugs!='null'  group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                CommunityTriageModel SList = new CommunityTriageModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (drInner["AgeText"].ToString() == "Below 18" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].above30 = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }

            con.Close();
            VM.HudWise = RListHud;

            return VM;
        }

        [HttpGet]
        [Route("GetDistrictdrugAgeWise")]
        public VMCommunityTriage getdistrictdrug()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select MS.district_id,MS.district_name,MS.district_gid from public.address_district_master as MS order by district_name";
            con.Open();
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
                cmdInner.CommandText = "select MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END AgeText,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id inner join public.health_screening as S  on M.member_id=S.member_id and s.drugs!='null'  group by MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END";

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
                            RList[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].above30 = drInner["TotalCount"].ToString();
                        }

                    }
                }

            }


            con.Close();
            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [Route("GetHUDdrugAgeWise")]
        public VMCommunityTriage gethuddrug()
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            cmdHud.CommandText = "select hud_id,hud_name,hud_gid from public.address_hud_master order by hud_name";

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
                cmdInner.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END AgeText from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id inner join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id inner join public.health_screening as S  on M.member_id=S.member_id and s.drugs!='null'  group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                CommunityTriageModel SList = new CommunityTriageModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (drInner["AgeText"].ToString() == "Below 18" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & RListHud[i].hud_gid == drInner["district_gid"].ToString())
                        {
                            RListHud[i].above30 = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }

            con.Close();
            VM.HudWise = RListHud;

            return VM;
        }

        [HttpGet]
        //[ResponseCache(Duration = 30 * 60)]
        [Route("Getdistrictpopulation")]
        public VMCommunityTriage districtpopulation([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select MS.district_name,MS.district_gid,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  group by MS.district_name,MS.district_gid";

            cmd.CommandText = "select MS.district_name,MS.district_gid,count(fm.family_id) TotalCount from  public.address_district_master as MS  \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id  " + CommunityParam + " \r\ngroup by MS.district_name,MS.district_gid";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                var SList = new CommunityTriageModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();
                SList.screeningCount = "0";
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
                cmdInner.CommandText = "select tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END AgeText,count(member_id) TotalCount from ( select MS.district_name,MS.district_gid,date_part('year',age(birth_date)) Age,M.member_id from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id  inner join public.health_screening as S  on M.member_id=S.member_id  group by MS.district_name,MS.district_gid,date_part('year',age(birth_date)),M.member_id) tbl group by tbl.district_name,tbl.district_gid,CASE  WHEN age between 0 and 17 THEN 'Below 18'  WHEN age between 18 and 29 THEN '18 to 30'  WHEN age between 30 and 120 THEN 'Above 30'  END";
                //cmdInner.CommandText = "select MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END AgeText,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.family_member_master as M on MS.district_id=M.district_id inner join public.health_screening as S  on M.member_id=S.member_id   group by MS.district_name,MS.district_gid,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30'        END";

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
                            RList[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & SList.district_gid == drInner["district_gid"].ToString())
                        {
                            RList[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & SList.district_gid == drInner["district_gid"].ToString())
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
                //cmdInner.CommandText = "select fm.district_id,district_name,district_gid,count(hh.member_id) TotalCount from health_screening hh  inner join family_master fm on hh.family_id=fm.family_id inner join address_district_master dm on fm.district_id=dm.district_id group by fm.district_id,district_name,district_gid";

                cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n group by fm.district_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n inner join address_district_master dm on tbl.district_id=dm.district_id \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

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
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select district_id,district_name,district_gid,count(member_id) TotalCount from (select fm.district_id,district_name,district_gid,member_id  from health_screening hh  inner join family_master fm on hh.family_id=fm.family_id inner join address_district_master dm on fm.district_id=dm.district_id group by fm.district_id,district_name,district_gid,member_id) tbl group by district_id,district_name,district_gid";

                cmdInner.CommandText = " select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh  \r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

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
                //cmdInner.CommandText = "select district_id,district_name,district_gid,count(member_id) TotalCount from (select fm.district_id,district_name,district_gid,member_id,count(screening_id) from health_screening hh  inner join family_master fm on hh.family_id=fm.family_id inner join address_district_master dm on fm.district_id=dm.district_id group by fm.district_id,district_name,district_gid,member_id having count(screening_id)=1) tbl group by district_id,district_name,district_gid";

                cmdInner.CommandText = "select tbl.district_id,district_name,district_gid,count(member_id) TotalCount from \r\n (select fm.district_id,district_name,district_gid,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER from health_screening hh\r\n inner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n group by fm.district_id,district_name,district_gid,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\n INNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\n group by tbl.district_id,district_name,district_gid";

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
                            RList[i].onescreening = drInner["TotalCount"].ToString();

                        }
                    }
                }
            }
            con.Close();
            for (int i = 0; i < RList.Count; i++)
            {
                RList[i].multiscreening = (int.Parse(RList[i].uniqueCount) - int.Parse(RList[i].onescreening)).ToString();
            }
            VM.DistrictWise = RList;

            return VM;
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("GetHUDScreenPopulationAgeWise")]
        public VMCommunityTriage gethudscreeningAge([FromQuery] FilterpayloadModel F)
        {
            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            Filterforall(F);

            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            //cmdHud.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id inner join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id  group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid";

            cmdHud.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(fm.family_id) TotalCount from  \r\npublic.address_district_master as MS  \r\ninner join public.address_hud_master as hu on MS.district_id=hu.district_id \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id and  hu.HUD_id=fm.HUD_id  " + CommunityParam + " \r\ngroup by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<HudModel> RListHud = new List<HudModel>();

            while (drHud.Read())
            {

                var SList = new HudModel();

                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                //SList.hud_id = drHud["hud_id"].ToString();
                SList.district_name = drHud["district_name"].ToString();
                SList.TotalCount = drHud["TotalCount"].ToString();

                SList.uniqueCount = "0";
                SList.screeningCount = "0";
                SList.onescreening = "0";

                RListHud.Add(SList);
            }


            con.Close();
            con.Open();
            if (RListHud.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,count(M.family_id) TotalCount,CASE             WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END AgeText from  public.address_district_master as MS  inner join public.address_hud_master as hu on MS.district_id=hu.district_id inner join public.family_member_master as M on MS.district_id=M.district_id and  hu.HUD_id=M.HUD_id inner join public.health_screening as S  on M.member_id=S.member_id  group by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,CASE      WHEN date_part('year',age(birth_date)) between 0 and 17 THEN 'Below 18'            WHEN date_part('year',age(birth_date)) between 18 and 29 THEN '18 to 30'            WHEN date_part('year',age(birth_date)) between 30 and 120 THEN 'Above 30' END";

                cmdInner.CommandText = "select tblFinal.district_name,tblFinal.hud_gid,tblFinal.HUD_name,tblFinal.district_gid,tblFinal.AgeText,sum(tblFinal.totalcount)TotalCount   from(select MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid\r\n,(fm.UPDATE_REGISTER)->0->> 'user_id' AS ARRUSER,count(fm.family_id) TotalCount,CASE             \r\nWHEN date_part('year',age(fm.birth_date)) between 0 and 17 THEN 'Below 18'            \r\nWHEN date_part('year',age(fm.birth_date)) between 18 and 29 THEN '18 to 30'            \r\nWHEN date_part('year',age(fm.birth_date)) between 30 and 120 THEN 'Above 30' END AgeText \r\nfrom  public.address_district_master as MS  \r\ninner join public.address_hud_master as hu on MS.district_id=hu.district_id \r\ninner join public.family_member_master as fm on MS.district_id=fm.district_id and  hu.HUD_id=fm.HUD_id  " + CommunityParam + " \r\ninner join public.health_screening as S  on fm.member_id=S.member_id  \r\ngroup by MS.district_name,hu.hud_gid,hu.HUD_name,MS.district_gid,ARRUSER,\r\nCASE      \r\nWHEN date_part('year',age(fm.birth_date)) between 0 and 17 THEN 'Below 18'            \r\nWHEN date_part('year',age(fm.birth_date)) between 18 and 29 THEN '18 to 30'            \r\nWHEN date_part('year',age(fm.birth_date)) between 30 and 120 THEN 'Above 30' END)tblFinal\r\nINNER JOIN USER_MASTER UM ON CAST(tblFinal.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by tblFinal.district_name,tblFinal.hud_gid,tblFinal.HUD_name,tblFinal.district_gid,tblFinal.AgeText";


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();


                CommunityTriageModel SList = new CommunityTriageModel();



                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (drInner["AgeText"].ToString() == "Below 18" & RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].below18 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "18 to 30" & RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].age_18_30 = drInner["TotalCount"].ToString();
                        }
                        else if (drInner["AgeText"].ToString() == "Above 30" & RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].above30 = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (RListHud.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select hm.hud_gid,count(member_id) TotalCount from (select fm.hud_id,member_id  from health_screening hh inner join family_master fm on hh.family_id=fm.family_id group by fm.hud_id,member_id) tbl inner join address_hud_master hm on hm.hud_id=tbl.hud_id group by hm.hud_gid";

                cmdInner.CommandText = "select hm.hud_gid,count(member_id) TotalCount from \r\n(select fm.hud_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\ninner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + "\r\ngroup by fm.hud_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id') tbl \r\ninner join address_hud_master hm on hm.hud_id=tbl.hud_id \r\nINNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by hm.hud_gid";

                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].uniqueCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();

            con.Open();
            if (RListHud.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select fm.hud_id,hm.hud_gid,count(member_id) TotalCount  from health_screening hh inner join family_master fm on hh.family_id=fm.family_id inner join address_hud_master hm on hm.hud_id=fm.hud_id group by fm.hud_id,hm.hud_gid";

                cmdInner.CommandText = "select fm.hud_id,hm.hud_gid,count(member_id) TotalCount  from \r\n(SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\nfamily_id,member_id  FROM PUBLIC.health_screening B\r\nWHERE JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,member_id,family_id) hh \r\ninner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + " \r\ninner join address_hud_master hm on hm.hud_id=fm.hud_id \r\nINNER JOIN USER_MASTER UM ON CAST(hh.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by fm.hud_id,hm.hud_gid";


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].screeningCount = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            con.Open();
            if (RListHud.Count > 0)
            {
                NpgsqlCommand cmdInner = new NpgsqlCommand();
                cmdInner.Connection = con;
                cmdInner.CommandType = CommandType.Text;
                //cmdInner.CommandText = "select hm.hud_gid,count(member_id) TotalCount from (select fm.hud_id,member_id,count(screening_id)  from health_screening hh inner join family_master fm on hh.family_id=fm.family_id group by fm.hud_id,member_id having count(screening_id)=1) tbl inner join address_hud_master hm on hm.hud_id=tbl.hud_id  group by hm.hud_gid";


                cmdInner.CommandText = "select hm.hud_gid,count(member_id) TotalCount from \r\n(select fm.hud_id,member_id,count(screening_id),JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' AS ARRUSER  from health_screening hh \r\ninner join family_master fm on hh.family_id=fm.family_id  " + CommunityParam + " \r\ngroup by fm.hud_id,member_id,JSONB_ARRAY_ELEMENTS(hh.UPDATE_REGISTER)->> 'user_id' having count(screening_id)=1) tbl \r\ninner join address_hud_master hm on hm.hud_id=tbl.hud_id  \r\nINNER JOIN USER_MASTER UM ON CAST(tbl.ARRUSER AS text) = cast(UM.USER_ID as text)\r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ngroup by hm.hud_gid";


                NpgsqlDataReader drInner = cmdInner.ExecuteReader();
                CommunityTriageModel SList = new CommunityTriageModel();
                while (drInner.Read())
                {
                    for (int i = 0; i < RListHud.Count; i++)
                    {
                        if (RListHud[i].hud_gid == drInner["hud_gid"].ToString())
                        {
                            RListHud[i].onescreening = drInner["TotalCount"].ToString();
                        }
                    }
                }
            }
            con.Close();
            for (int i = 0; i < RListHud.Count; i++)
            {
                RListHud[i].multiscreening = (int.Parse(RListHud[i].uniqueCount) - int.Parse(RListHud[i].onescreening)).ToString();
            }

            VM.HudWise = RListHud;

            return VM;
        }

        #region dashboard
        //Dashboard Count
        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration =30 * 60)]
        [Route("gettotalpopulation")]
        public Object gettotalpopulation([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";


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

                    Disparam = "where " + Disparam;

                }
                else
                {
                    Disparam = "where (fm.district_id = '" + F.district_id + "')";
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

                    if (F.district_id != "")
                    {
                        Disparam = "and " + Disparam;
                    }
                    else
                    {
                        Disparam = "where " + Disparam;
                    }



                }
                else
                {
                    if (F.district_id != "")
                    {
                        Disparam = "and " + "(fm.hud_id = '" + F.hud_id + "')";
                    }
                    else
                    {
                        Disparam = "where (fm.hud_id = '" + F.hud_id + "')";
                    }
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


                    if (F.district_id != "" || F.hud_id != "")
                    {
                        Disparam = "and " + Disparam;
                    }
                    else
                    {
                        Disparam = "where " + Disparam;
                    }



                }
                else
                {
                    if (F.district_id != "" || F.hud_id != "")
                    {
                        Disparam = "and " + "(fm.block_id = '" + F.block_id + "')";
                    }
                    else
                    {
                        Disparam = "where (fm.block_id = '" + F.block_id + "')";
                    }

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

                    if (F.district_id != "" || F.hud_id != "" || F.block_id != "")
                    {
                        Disparam = "and " + Disparam;
                    }
                    else
                    {
                        Disparam = "where " + Disparam;
                    }



                }
                else
                {
                    if (F.district_id != "" || F.hud_id != "" || F.block_id != "")
                    {
                        Disparam = "and " + "(fm.facility_id = '" + F.facility_id + "')";
                    }
                    else
                    {
                        Disparam = "where (fm.facility_id = '" + F.facility_id + "')";
                    }


                }



                CommunityParam = CommunityParam + Disparam;
            }

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select count(M.family_id) TotalCount from  public.family_member_master  M";

            cmd.CommandText = "select count(fm.family_id) TotalCount from  public.family_member_master fm " + CommunityParam + "";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }



        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gettotalscreening")]
        public Object gettotalscreening([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select count(member_id) TotalCount from   public.health_screening as S";

            cmd.CommandText = "select count(member_id) TotalCount from   public.health_screening as S inner join family_master fm on S.family_id=fm.family_id  " + CommunityParam + "";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }
        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gettotalindscreening")]
        public Object gettotalindscreening([FromQuery] FilterpayloadModel F)
        {
            Filterforall(F);

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = " select count(tbl.member_id) TotalCount from (select member_id from   public.health_screening as S   group by member_id) tbl";

            cmd.CommandText = " select count(tbl.member_id) TotalCount from (select member_id from   public.health_screening as S inner join family_master fm on S.family_id=fm.family_id " + CommunityParam + "  group by member_id) tbl";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gettotaldrug")]
        public Object gettotaldrug([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(member_id) TotalCount from  public.health_screening as S inner join family_master fm on S.family_id=fm.family_id  " + CommunityParam + " where S.drugs!='null'";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gettotalinddrug")]
        public Object gettotalinddrug([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(member_id) TotalCount from (select member_id from  public.health_screening as S inner join family_master fm on S.family_id=fm.family_id  " + CommunityParam + " where S.drugs!='null' group by member_id) tbl";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }


        [HttpGet]
        [Route("gettotalmtmbenfvget")]
        public Object gettotalmtmbenfvget(string district_id)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            //Filterforall(F);

            string Param = "";

            if (district_id != null)
            {
                Param = "and fm.district_id=" + district_id + "";
            }

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(member_id) TotalCount from health_history S inner join family_master fm on S.family_id=fm.family_id  " + Param + " where CAST (mtm_beneficiary -> 'avail_service' as text)='\"yes\"' ";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("gettotalmtmbenf")]
        public Object gettotalmtmbenf([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(member_id) TotalCount from health_history S inner join family_master fm on S.family_id=fm.family_id  " + CommunityParam + " where CAST (mtm_beneficiary -> 'avail_service' as text)='\"yes\"' ";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getmtmdrugissues")]
        public Object getmtmdrugissues([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select count(hs.member_id) Totalcount from health_history h  inner join health_screening hs on h.member_id=hs.member_id inner join family_master fm on h.family_id=fm.family_id  " + CommunityParam + "and hs.drugs!='null' where CAST (mtm_beneficiary -> 'avail_service' as text)='\"yes\"'";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<CommunityTriageModel> RList = new List<CommunityTriageModel>();

            while (dr.Read())
            {
                TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            return new { totalPopulation = TotalPopulation };
        }

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("rolewisescreening")]
        public List<RoleReport> rolewisescreening([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();
            string TotalPopulation = "0";
            con.Open();

            Filterforall(F);

            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select role_name,sum(userCount) desigcount from  (select tbl.arruser->>'user_id' as user_id,count(screening_id)as userCount from (SELECT jsonb_array_elements(b.update_register) AS arruser,screening_id FROM   public.health_screening b WHERE  jsonb_typeof(b.update_register) = 'array') tbl  group by user_id) tbl inner join user_master um on cast(tbl.user_id as uuid)=um.user_id inner join user_role_Master urm on urm.role_id=um.role where tbl.user_id!='system' group by role_name order by desigcount desc limit 10";

            cmd.CommandText = "select role_name,sum(userCount) desigcount from  \r\n(select tbl.family_id,tbl.arruser->>'user_id' as user_id,count(screening_id)as userCount from \r\n(SELECT jsonb_array_elements(b.update_register) AS arruser,family_id,screening_id \r\nFROM public.health_screening b WHERE  jsonb_typeof(b.update_register) = 'array') tbl  group by user_id,tbl.family_id) tbl \r\ninner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + "\r\ninner join user_master um on cast(tbl.user_id as uuid)= um.user_id \r\nINNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  " + InstitutionParam + "\r\ninner join user_role_Master urm on urm.role_id = um.role \r\nwhere tbl.user_id != 'system' group by role_name order by desigcount desc limit 10";


            //cmd.CommandText = "select role_name,sum(userCount) desigcount from  (select tbl.arruser->>'user_id' as user_id,count(screening_id)as userCount from (SELECT jsonb_array_elements(b.update_register) AS arruser,screening_id FROM   public.health_screening b WHERE  jsonb_typeof(b.update_register) = 'array') tbl --inner join user_master um on um.user_id=cast(tbl.arruser -> 'user_id' as uuid) group by user_id) tbl inner join user_master um on cast(tbl.user_id as uuid)=um.user_id inner join user_role_Master urm on urm.role_id=um.role where tbl.user_id!='system' group by role_name order by desigcount desc limit 10";

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<RoleReport> RList = new List<RoleReport>();
            int ReuseCount = 0;

            while (dr.Read())
            {
                RList.Add(new RoleReport
                {
                    RoleName = dr["role_name"].ToString(),
                    RoleCount = dr["desigcount"].ToString(),
                });
                ReuseCount = ReuseCount + int.Parse(dr["desigcount"].ToString());
                //TotalPopulation = dr["TotalCount"].ToString();
            }
            con.Close();
            foreach (var aa in RList) { aa.CountPer = Percentage_Cal(ReuseCount, int.Parse(aa.RoleCount)); }
            return RList;
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
        #endregion

        [HttpGet]
        [ResponseCache(Duration = 30 * 60)]
        [OutputCache(Duration = 30 * 60)]
        [Route("getdistrictmtm")]
        public getDrugListModelList getdistrictmtm([FromQuery] FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            getDrugListModelList VM = new getDrugListModelList();

            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandType = CommandType.Text;
            //cmd.CommandText = "select fm.district_id,district_name,district_gid,count(hh.member_id) TotalCount from health_history hh  inner join family_master fm on hh.family_id=fm.family_id inner join address_district_master dm on fm.district_id=dm.district_id where CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' group by fm.district_id,district_name,district_gid";


            Filterforall(F);

            cmd.CommandText = String.Format("select * from public.getdrugdistrict('" + CommunityParam + "','" + InstitutionParam + "')");

            NpgsqlDataReader dr = cmd.ExecuteReader();
            List<GetDrugdistrictModel> RList = new List<GetDrugdistrictModel>();

            while (dr.Read())
            {

                var SList = new GetDrugdistrictModel();

                SList.district_name = dr["district_name"].ToString();
                SList.district_gid = dr["district_gid"].ToString();
                SList.TotalCount = dr["TotalCount"].ToString();

                RList.Add(SList);
            }
            con.Close();

           VM.DistrictWise = RList;

            return VM;
        }

        [HttpPost]
        [Route("gethudmtm")]
        public VMCommunityTriage gethudmtm(FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            Filterforall(F);

            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            //cmdHud.CommandText = "select fm.district_id,district_name,district_gid,hud_name,hud_gid,count(hh.member_id) TotalCount from health_history hh  inner join family_master fm on hh.family_id=fm.family_id inner join address_district_master dm on fm.district_id=dm.district_id inner join address_hud_master hm on hm.hud_id=fm.hud_id where CAST (mtm_beneficiary -> 'avail_service' as text)='\"yes\"' group by fm.district_id,district_name,district_gid,hud_name,hud_gid";

            cmdHud.CommandText = "select fm.district_id,district_name,district_gid,hud_name,hud_gid,count(member_id) TotalCount from \r\n (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\n family_id,member_id,medical_history_id  FROM PUBLIC.health_history B\r\n WHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,member_id,medical_history_id) tbl\r\n inner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + "  \r\n inner join address_district_master dm on fm.district_id=dm.district_id \r\n inner join address_hud_master hm on hm.hud_id=fm.hud_id INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  \r\n " + InstitutionParam + " group by fm.district_id,district_name,district_gid,hud_name,hud_gid";


            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<HudModel> RListHud = new List<HudModel>();

            while (drHud.Read())
            {

                var SList = new HudModel();

                SList.hud_name = drHud["hud_name"].ToString();
                SList.hud_gid = drHud["hud_gid"].ToString();
                //SList.hud_id = drHud["hud_id"].ToString();
                SList.district_gid = drHud["district_gid"].ToString();
                SList.district_name = drHud["district_name"].ToString();
                SList.TotalCount = drHud["TotalCount"].ToString();

                RListHud.Add(SList);
            }
            con.Close();
            VM.HudWise = RListHud;
            return VM;
        }

        [HttpPost]
        [Route("getblockmtm")]
        public List<BlockModel> getblockmtm(FilterpayloadModel F)
        {

            NpgsqlConnection con = new NpgsqlConnection(_configuration.GetConnectionString("Constring"));
            VMCommunityTriage VM = new VMCommunityTriage();

            Filterforall(F);

            ///*Hud Wise*/
            con.Open();
            NpgsqlCommand cmdHud = new NpgsqlCommand();
            cmdHud.Connection = con;
            cmdHud.CommandType = CommandType.Text;
            //cmdHud.CommandText = "select fm.block_id,count(hh.member_id) TotalCount  from health_history hh   inner join family_master fm on hh.family_id=fm.family_id where CAST (mtm_beneficiary ->> 'avail_service' as text)='yes'  group by fm.block_id";


            cmdHud.CommandText = "select fm.block_id,count(member_id) TotalCount from \r\n (SELECT JSONB_ARRAY_ELEMENTS(B.UPDATE_REGISTER)->> 'user_id' AS ARRUSER, \r\n family_id,member_id,medical_history_id  FROM PUBLIC.health_history B\r\n WHERE CAST (mtm_beneficiary ->> 'avail_service' as text)='yes' and JSONB_TYPEOF(B.UPDATE_REGISTER) = 'array' GROUP BY ARRUSER,member_id,medical_history_id) tbl\r\n inner join family_master fm on tbl.family_id=fm.family_id  " + CommunityParam + "  \r\n INNER JOIN USER_MASTER UM ON CAST(TBL.ARRUSER AS text) = cast(UM.USER_ID as text)\r\n INNER JOIN FACILITY_REGISTRY FR ON FR.FACILITY_ID = UM.FACILITY_ID  \r\n " + InstitutionParam + " group by fm.block_id";

            NpgsqlDataReader drHud = cmdHud.ExecuteReader();
            List<BlockModel> RListHud = new List<BlockModel>();

            while (drHud.Read())
            {

                var SList = new BlockModel();

                SList.block_id = drHud["block_id"].ToString();

                SList.TotalCount = drHud["TotalCount"].ToString();

                RListHud.Add(SList);
            }
            con.Close();
            //VM.HudblockWise = RListHud;
            return RListHud;
        }
    }
}
