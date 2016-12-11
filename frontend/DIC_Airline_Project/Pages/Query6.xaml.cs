using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DIC_Airline_Project.Pages
{
    /// <summary>
    /// Interaction logic for Query6.xaml
    /// </summary>
    public partial class Query6 : Page
    {
        public Query6(string url)
        {
            InitializeComponent();
            LoadJsonData(url);
        }

        private void LoadJsonData(string url)
        {
            var client = new RestClient(url);
            var request = new RestRequest(Method.GET);
            request.OnBeforeDeserialization = resp => { resp.ContentType = "application/json"; };
            var response = client.Execute<List<Entities.JsonServiceResult2>>(request);
            string content = response.Content.ToString();
            content = content.Substring(1, content.Length - 2);
            content = content.Replace(@"[", "");
            content = content.Replace(@"]", "");
            BitmapImage bi3 = new BitmapImage();          
            

            if (content == "0.0")
            {
                ResultBlock.Text = "Woohoo!!! There will be no delay for the carrier";
                bi3.BeginInit();
                bi3.UriSource = new Uri("/Images/happy.jpg", UriKind.Relative);
                bi3.EndInit();
                
            }
            if (content == "1.0")
            {
                ResultBlock.Text = "Oh No!!! There might be high probability of delay for the carrier";
                bi3.BeginInit();
                bi3.UriSource = new Uri("/Images/sad.png", UriKind.Relative);
                bi3.EndInit();
              
            }

            ResultImage.Source = bi3;

        }
    }
}
