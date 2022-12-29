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

-- Look at the total cases vs. total deaths by country:
-- Shows likelihood of dying if you contract COVID in your country:
SELECT Location, date, total_cases, total_deaths, round((total_deaths/total_cases)*100,2) as death_percentage
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
order by 1,2;

-- Look at the total cases vs. total deaths in the United States:
-- Shows likelihood of dying if you contract COVID in the United States:
SELECT Location, date, total_cases, total_deaths, round((total_deaths/total_cases)*100,2) as death_percentage
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and location like '%state%' and continent is not null
order by 1,2;

-- Looking at Total Cases vs. Population
-- Shows what percentage of population contracted COVID
SELECT Location, date, population, total_cases, round((total_cases/population)*100,2) as Percent_population_infected
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and location like '%state%' and continent is not null
order by 1,2;

-- What countries have the highest infection rates compared to their population?
SELECT Location, population, max(total_cases) as highest_infection_count_recorded, round(max((total_cases)/population)*100,2) as Percent_population_infected
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
group by location, population
order by Percent_population_infected desc

-- Showing countries with highest death count per population
SELECT Location, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is not null
group by location
order by total_death_count desc;

-- LET'S BREAK THINGS DOWN BY LOCAION
SELECT location, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is null
group by location
order by total_death_count desc;

-- LET'S BREAK THINGS DOWN BY CONTINENT...

-- Showing Continents with the highest death counts
SELECT continent, max(total_deaths) as total_death_count
FROM Portfolio_Project..Covid_Deaths$
where total_deaths is not null and continent is NOT null
group by continent
order by total_death_count desc;

-- GLOBAL NUMBERS

Select date, sum(new_cases)--round((total_deaths/total_cases)*100,2) as death_percentage
from Portfolio_Project..Covid_Deaths$
where continent is not null
and total_deaths is not null
group by date
order by 1,2;

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

-- Looking at Rolling Vaccinations by location and date
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 1,2,3;




-- Looking at Total Populations vs Vaccinations (2 methods)


-- Setup:
 Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations,
	   -- (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
order by 1,2,3;


-- Method 1: Use CTE 
With Pop_vs_vac (continent, location, date, population, new_vaccinations, rolling_vaccinations)
as(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	   sum(convert(bigint,vac.new_vaccinations)) OVER (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
	   -- (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null and vac.new_vaccinations is not null
--order by 1,2,3
)
Select*, (rolling_vaccinations/population)*100 as percent_rolling_vaccinations
from
Pop_vs_vac;


-- Method 2: Use Temp Table

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
drop table if exists #PercentPopulationVaccinated
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
--where dea.continent is not null and vac.new_vaccinations is not null
--order by 1,2,3
Select*, (Rolling_Vaccinations/population)*100 as percent_rolling_vaccinations
from #PercentPopulationVaccinated
Pop_vs_vac;



-- Create View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
sum(Convert(bigint, vac.new_vaccinations)) over (Partition by dea.location order by dea.location, dea.date) as rolling_vaccinations
--, (rolling_vaccinations/population)*100
from Portfolio_Project..Covid_Deaths$ dea
join Portfolio_Project..Covid_Vaccinations$ vac
	on dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
--order by 2,3

select * from PercentPopulationVaccinated