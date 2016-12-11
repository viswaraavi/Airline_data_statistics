using Newtonsoft.Json;
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
    /// Interaction logic for Query3.xaml
    /// </summary>
    public partial class Query3 : Page
    {
        List<Entities.JsonServiceResult3> ResultList;
        public Query3(string url)
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
            content = "[" + content.Replace(@"\", "") + "]";
            ResultList = JsonConvert.DeserializeObject<List<Entities.JsonServiceResult3>>(content);
            this.dataGrid.ItemsSource = ResultList;

        }
    }
}
