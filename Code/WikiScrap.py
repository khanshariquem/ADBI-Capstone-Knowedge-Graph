import scrapy
import re
import pandas as pd
import csv
import json

#Used Scrapy to extract information of movies present in the movies metadata data set. 

class WikiScrap(scrapy.Spider):
    name = 'wiki'
    result = []
    pd.set_option('display.max_columns', None)
    #movies = open('data.tsv').read().split('\n')
    df = pd.read_csv('movies_metadata.csv', header=0)
    #df = df.sort_values('titleId',ascending=False)
    #print(len(df))
    #print(df.head(10))
    #print(df.tail(10))
    staticURL = 'https://en.wikipedia.org/wiki/'
    start_urls = []
    movie = ""
    for index, row in df.iterrows():
        movie = row['original_title'] 
        url = staticURL + movie
        start_urls.append(url)
   
    def parse(self, response):
        
        data = dict()
        #index_meta = response.meta['index']
        firstHeading = response.xpath(".//*[@id='firstHeading']//text()").extract()
#         body = response.xpath(".//*[@id='bodyContent']//text()").extract()
#         body = self.clean_data(body)

        data['title'] = ' '.join(firstHeading)
        
        #infobox vevent
        #directed by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Directed by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Directed by'] = temp
        
        #infobox vevent
        #directed by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Produced by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Produced by'] = temp
        
        #infobox vevent
        #Written by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Written by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Written by'] = temp
        
        #infobox vevent
        #Starring
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Starring')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Starring'] = temp
        
        #infobox vevent
        #Music by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Music by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Music by'] = temp
        
        #infobox vevent
        #Cinematography
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Cinematography')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Cinematography'] = temp
        
        #infobox vevent
        #Edited by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Edited by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Edited by'] = temp
        
        #infobox vevent
        #Production
        temp = []
        xp = "..//table[@class='infobox vevent']//div[contains(text(), 'Production')]//parent::th//parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Production'] = temp
        
        #infobox vevent
        #Distributed by
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Distributed by')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Distributed by'] = temp
        
        #infobox vevent
        #Release
        temp = []
        xp = ".//table[@class='infobox vevent']//div[contains(text(), 'Release date')]//parent::th//parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Release date'] = temp
        
        #infobox vevent
        #Running time
        temp = []
        xp = ".//table[@class='infobox vevent']//div[contains(text(), 'Running time')]//parent::th//parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Running time'] = temp
        
        #infobox vevent
        #Country
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Country')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Country'] = temp
        
        #infobox vevent
        #Language
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Language')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Language'] = temp
        
        #infobox vevent
        #Budget
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Budget')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Budget'] = temp
        
        #infobox vevent
        #Box office
        temp = []
        xp = ".//table[@class='infobox vevent']//th[contains(text(), 'Box office')]/parent::tr//td//text()"
        temp += response.xpath(xp).extract()
        temp = ','.join(temp)
        temp = self.clean_data(temp)
        data['Box office'] = temp
  
        count = 0
        body = []
        while(count<15):
            xp = ".//*[@id='bodyContent']//p["+str(count) +']//text()'
            body += response.xpath(xp).extract()
            count += 1
        body = ','.join(body)
        body = self.clean_body_data(body)
        data['Summary'] = body
        
		#only add the movies for which the director related to dirctor is availbale . In short some information of the movie is available
        if data['Directed by'] != "":
            yield data
#             filename = data['title'] + '.txt'
#             with open(filename, 'w') as outfile:  
#                 json.dump(data, outfile)
            
        
        #print(data)
    #use regex to clean the data with special characters 
    def clean_body_data(self, body):
        return re.sub("[^a-zA-Z0-9 ,.[]]+", "", str(body))
    #use regex to clean the data 
    def clean_data(self, body):
        return re.sub("[^a-zA-Z0-9 ,]+", "", str(body))
    
    
    

