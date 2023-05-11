namespace PHRLockerAPI.Models
{
    public class DiagnosisReport
    {
        public string diagnosisId { get; set; }
        public string diagnosisName { get; set; }       
        public double totalop { get; set; }
        public double adultopmale { get; set; }
        public double adultopfemale { get; set; }
        public double adultoptransgender { get; set; }
        public double adulttotal { get; set; }

        public double childopmale { get; set; }
        public double childopfemale { get; set; }
        public double childoptransgender { get; set; }
        public double childtotal { get; set; }

        public double totalopmale { get; set; }
        public double totalopfemale { get; set; }
        public double totaloptransgender { get; set; }

    }
}
