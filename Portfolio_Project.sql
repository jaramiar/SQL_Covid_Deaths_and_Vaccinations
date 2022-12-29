

-- BEGIN EXPLORING THE DATASETS (Covid_Deaths & Covid_Vaccinations)

SELECT * 
FROM Portfolio_Project..Covid_Deaths$
where continent is not null
order by 3,4;

--SELECT * 
--FROM Portfolio_Project..Covid_Vaccinations$
--order by 3,4;


-- Select the data that we'll use, ordered by 'Location' and 'date':


SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
order by 1,2;



-- Total cases vs. total deaths by country
-- (Shows likelihood of dying if you contract COVID in your country)


SELECT Location, date, total_cases, total_deaths, round((total_deaths/total_cases)*100,2) as death_percentage
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
order by 1,2;


-- Total cases vs. total deaths in the United States
-- (Ssows likelihood of dying if you contract COVID in the United States)


SELECT Location, date, total_cases, total_deaths, round((total_deaths/total_cases)*100,2) as death_percentage
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and location like '%state%' and continent is not null
order by 1,2;


-- TOTAL CASES VS. POPULATIONS


-- What percentage of population contracted COVID?


SELECT Location, date, population, total_cases, round((total_cases/population)*100,2) as Percent_population_infected
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and location like '%state%' and continent is not null
order by 1,2;


-- Countries with the highest infection rates compared to their population


SELECT Location, population, max(total_cases) as highest_infection_count_recorded, round(max((total_cases)/population)*100,2) as Percent_population_infected
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
group by location, population
order by Percent_population_infected desc

-- Countries with highest death count per population


SELECT Location, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
group by location
order by total_death_count desc;


-- LET'S BREAK THINGS DOWN BY LOCATION

-- Death count vs. Location


SELECT location, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is null
group by location
order by total_death_count desc;


-- LET'S BREAK THINGS DOWN BY CONTINENT...


-- Continents with the highest death counts


SELECT continent, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is NOT null
group by continent
order by total_death_count desc;


-- Global numbers

-- Count of new cases by date
Select date, sum(new_cases)--round((total_deaths/total_cases)*100,2) as death_percentage
from Portfolio_Project..Covid_Deaths$
where continent is not null
and total_deaths is not null
group by date
order by 1,2;

-- Total cases, total deaths, and percent of cases resulting in death according to date
Select 
date, 
sum(new_cases) as total_cases, 
sum(cast(new_deaths as int)) as total_deaths, 
round((sum(new_deaths)/sum(new_cases))*100,2) as death_percentage
from Portfolio_Project..Covid_Deaths$
where continent is not null
group by date
order by 1,2;



-- COVID VACCINATIONS TABLE

Select *
from Portfolio_Project..Covid_Deaths$;


-- Looking at Rolling Vaccinations Count by location and date


Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 1,2,3;



-- LOOKING AT TOTAL VACCINATIONS Vs POPULATIONS (2 methods)


-- Setup:


Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
	   -- (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 1,2,3;


-- Method 1: Use a CTE 


With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Portfolio_Project..Covid_Deaths$ dea
Join Portfolio_Project..Covid_Vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3
)
Select *, (RollingPeopleVaccinated/Population)*100 as PercentPeopleVaccinated
From PopvsVac


-- Method 2: Use a Temp Table


Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_Vaccinations numeric,
Rolling_Vaccinations numeric
)
insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
	   -- (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
--order by 1,2,3
Select*, (Rolling_Vaccinations/population)*100 as percent_rolling_vaccinations
from #PercentPopulationVaccinated
Pop_vs_vac;


-- Alternative Temp Table 


drop table if exists #PercentPopulationVaccinated2
Create Table #PercentPopulationVaccinated2
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_Vaccinations numeric,
Rolling_Vaccinations numeric
)
insert into #PercentPopulationVaccinated2
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
	   -- (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null and vac.new_vaccinations is not null
--order by 1,2,3
Select*, (Rolling_Vaccinations/population)*100 as percent_rolling_vaccinations
from #PercentPopulationVaccinated2
Pop_vs_vac;



-- Create View to store data for later visualizations

Create View PercentPopulationVaccinated_view as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
sum(Convert(bigint, vac.new_vaccinations)) over (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
--, (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
--order by 2,3

select * from PercentPopulationVaccinated_view






-- Queries used for Tableau Project


-- 1. 

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From Portfolio_Project..Covid_Deaths$
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2

-- Just a double check based off the data provided
-- numbers are extremely close so we will keep them - The Second includes "International"  Location


--Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
--From PortfolioProject..CovidDeaths
----Where location like '%states%'
--where location = 'World'
----Group By date
--order by 1,2


-- 2. 

-- We take these out as they are not inluded in the above queries and want to stay consistent
-- European Union is part of Europe

Select location, SUM(cast(new_deaths as int)) as TotalDeathCount
From Portfolio_Project..Covid_Deaths$
--Where location like '%states%'
Where continent is null 
and location not in ('World', 'European Union', 'International','High income','Upper middle income','Lower middle income','Low income')
Group by location
order by TotalDeathCount desc


-- 3.

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Portfolio_Project..Covid_Deaths$
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- 4.


Select Location, Population,date, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Portfolio_Project..Covid_Deaths$
--Where location like '%states%'
Group by Location, Population, date
order by PercentPopulationInfected desc
