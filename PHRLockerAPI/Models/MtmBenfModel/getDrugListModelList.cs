namespace PHRLockerAPI.Models.MtmBenfModel
{
    public class getDrugListModelList
    {

        public List<GetDrugdistrictModel> DistrictWise { get; set; }


    }

    public class GetDrugdistrictModel
    {

        public string district_name { get; set; }

        public string district_gid { get; set; }
        public string TotalCount { get; set; }

    }
}
