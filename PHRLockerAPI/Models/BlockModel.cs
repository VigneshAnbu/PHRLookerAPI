namespace PHRLockerAPI.Models
{
    public class BlockModel
    {
        public string block_id { get; set; }

        public string RiskScore { get; set; }
        public string block_name { get; set; }

        public string block_gid { get; set; }

        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string ScreeningValues { get; set; }

        public string NormalRisk { get; set; }

        public string MediumRisk { get; set; }

        public string LowRisk { get; set; }

        public string HighRisk { get; set; }

        public string TotalCount { get; set; }

        public string hud_name { get; set; }

        public string hud_gid { get; set; }
        public string hud_id { get; internal set; }
    }
}
