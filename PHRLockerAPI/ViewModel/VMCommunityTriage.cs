using PHRLockerAPI.Models;

namespace PHRLockerAPI.ViewModel
{
    public class VMCommunityTriage
    {
        public List<CommunityTriageModel> DistrictWise { get; set; }

        public List<HudModel> HudWise { get; set; }

        public List<BlockModel> BlockWise { get; set; }

        public List<VillageModel> VillageWise { get; set; }

        public string Normal { get; set; }

        public string Medium { get; set; }

        public string Low { get; set; }

        public string High { get; set; }
    }
}
