namespace PHRLockerAPI.Models
{
    public class HudModel
    {
        public string below18 { get; set; }
        public string age_18_30 { get; set; }
        public string above30 { get; set; }

        public string hud_id { get; set; }

        public string RiskScore { get; set; }
        public string district_name { get; set; }

        public string district_gid { get; set; }

        public string hud_name { get; set; }

        public string hud_gid { get; set; }

        public string ScreeningValues { get; set; }

        public string NormalRisk { get; set; }

        public string MediumRisk { get; set; }

        public string LowRisk { get; set; }

        public string HighRisk { get; set; }
        public string TotalCount { get; set; }

        public string screeningCount { get; set; }
        public string uniqueCount { get; set; }
        public string onescreening { get; set; }
        public string multiscreening { get; set; }

        public string district_id { get; set; }
    }
}
