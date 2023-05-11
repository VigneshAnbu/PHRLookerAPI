namespace PHRLockerAPI.Intfa
{
    public class smsintegra : Ismsgateway
    {
        public string textsms { get; set; }
        public smsintegra()
        {
            this.textsms = "smsintegra";
        }
        public void sednsmstouser(string sms,string mobileno)
        {
            //sms message
        }
    }
}
