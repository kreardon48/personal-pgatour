name = 'webscrapers'

import pandas as pd
import time
from selenium.webdriver.chrome      import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by   import By


def wiki_tournament_search(yearIn, tournamentIn):
    """
    Automating Google Searching w/ Selenium to Standardize
    Tournament Names, and Tournament Courses.
    
    Also will provide historical grouping of Tournaments where
    Tournament Names have changed over the Years.
    
    Parameters
    ----------
    yearIn : string
        The year in which the tournament took or will take place, as shown
        in tournamentEndpoint in SportsDataIO.
    tournamentIn : string
        The tournamentName as shown in tournamentEndpoint in SportsDataIO.

    Returns
    -------
    list
        Contains the tournament year, tournament name as entered in the Google
        search, as well as the first Google Search Webpage Name and Webpage URL.

    """
    
    driver = webdriver.WebDriver()
    driver.get("https://www.google.com")
    
    search_box = driver.find_element(By.NAME,'q')
    search_query = f"{yearIn} {tournamentIn} golf wikipedia"
    search_box.send_keys(search_query)
    
    search_box.send_keys(Keys.RETURN)
    
    time.sleep(5)
    
    search_results = driver.find_element(By.CSS_SELECTOR, ".tF2Cxc")
    results = search_results.find_element(By.CSS_SELECTOR, ".DKV0Md").text
    hrefLink = search_results.find_element(By.CSS_SELECTOR, ".yuRUbf a").get_attribute("href")
    driver.quit()
    return [yearIn, tournamentIn, results, hrefLink]


def espn_pga_schedule_extract(seasonIn):
    """
    
    Parameters
    ----------
    seasonIn : string
        The season, as "YYYY-YY" for all previous, non-current, years in which we want to extract
        the tournament level information for, from ESPN.

    Returns
    -------
    dfOutput : pd.DataFrame
        The tournament level data that is scraped from the resulting webpage.

    """
    seasonOut = seasonIn[:2] + str(int(seasonIn[-2:]) - 1) + "-" + seasonIn[-2:]
    driver = webdriver.WebDriver()
    driver.get(f"https://www.espn.com/golf/schedule/_/season/{seasonIn}")
    
    driver_results = driver.find_elements(By.CLASS_NAME, "ResponsiveTable")
    
    dfOutput = pd.DataFrame()
    for table in driver_results:
        table_headers  = [header.text for header in table.find_elements(By.CLASS_NAME, "Table__TH")]
        table_rows     = table.find_elements(By.CLASS_NAME, "Table__TR")
        row_data = []
        for row in table_rows:
            cell_data = []
            for cell in row.find_elements(By.CLASS_NAME, "Table__TD"):
                cell_data.append(cell.text)
            if cell_data == []:
                pass
            else:
                row_data.append(cell_data)
        
        dfTmp = pd.DataFrame([], columns = table_headers).assign(Season = '')
        for row in row_data:
            d01 = pd.DataFrame(row).transpose()
            d01.columns = table_headers
            d01['Season'] = f"{seasonOut}"
            dfTmp = pd.concat([dfTmp, d01], sort = False)
        
        dfOutput = pd.concat([dfOutput, dfTmp], sort = False)
    driver.quit()
    return dfOutput