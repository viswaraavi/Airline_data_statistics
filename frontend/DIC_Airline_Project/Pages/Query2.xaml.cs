using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;

using System.Windows.Controls;


namespace DIC_Airline_Project.Pages
{
    /// <summary>
    /// Interaction logic for Query2.xaml
    /// </summary>
    /// 
    
    public partial class Query2 : Page
    {
        List<Entities.JsonServiceResult2> ResultList;

        public Query2(string url)
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
            content = "["+ content.Replace(@"\", "") + "]";
            ResultList = JsonConvert.DeserializeObject<List<Entities.JsonServiceResult2>>(content);
            this.dataGrid.ItemsSource = ResultList;    
        }
    }
}
