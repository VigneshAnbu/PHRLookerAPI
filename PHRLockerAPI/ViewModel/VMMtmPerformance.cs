using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class VMMtmPerformance
    {
        public List<GenderWise> GenderList { get; set; }

        public List<AgeWiseModel> AgeList { get; set; }

        public List<WeekModel> WeekList { get; set; }

        public List<BlockWiseModel> BlockList { get; set; }

        public List<BlockWiseModel> MTMBlockList { get; set; }

        public List<WeekModel> MTMWeekList { get; set; }

        public List<WeekModel> MTMWeekListDrug { get; set; }

        public List<DrugModel> DrugList { get; set; }

        public string Middle { get; set; }

        public string Above { get; set; }

        public string Below { get; set; }



    }
}
