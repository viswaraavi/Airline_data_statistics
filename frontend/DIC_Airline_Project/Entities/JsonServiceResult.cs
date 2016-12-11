using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DIC_Airline_Project.Entities
{
    class JsonServiceResult1
    {
        public double carrer_delay { get; set; }
        public double weather_delay { get; set; }
        public double nas_delay { get; set; }
        public double security_delay { get; set; }
        public double late_aircraft { get; set; }
    }
    class JsonServiceResult2
    {
        public string CARRIER { get; set; }
        public double ARR_DELAY_AVG { get; set; }
        public double DEP_DELAY_AVG { get; set; }

    }
    class JsonServiceResult3
    {
        public string CARRIER { get; set; }
        public double CANCELLED_TOTAL { get; set; }
        public double TOTAL { get; set; }        
    }

    class JsonServiceResult4
    {
        public string CARRIER { get; set; }
        public string FL_NUM { get; set; }
        public string ORIGIN_CITY_NAME { get; set; }
        public string DEST_CITY_NAME { get; set; }
        public string Air_Time { get; set; }

    }


    class JsonServiceResult5
    {
        public string MONTH { get; set; }
        public double ARR_DELAY_AVG { get; set; }
        public double DEP_DELAY_AVG { get; set; }
    }
      

}
