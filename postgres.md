classDiagram
direction BT
class articles {
   varchar(255) title
   text summary
   varchar(2048) website
   text content
   text[] keywords
   date date
   integer number_dead
   integer number_missing
   integer number_survivors
   varchar(255) country_of_origin
   varchar(255) region_of_origin
   text cause_of_death
   varchar(255) region_of_incident
   varchar(255) country_of_incident
   text location_of_incident
   numeric(9,6) latitude
   numeric(9,6) longitude
   boolean relevant
   integer article_id
}
class incidents {
   varchar(255) title
   boolean verified
   date date
   integer number_dead
   integer number_missing
   integer number_survivors
   varchar(255) country_of_origin
   varchar(255) region_of_origin
   text cause_of_death
   varchar(255) region_of_incident
   varchar(255) country_of_incident
   text location_of_incident
   numeric(9,6) latitude
   numeric(9,6) longitude
   integer incident_id
}
class mapping {
   integer incident_id
   integer article_id
}

mapping  -->  articles : article_id
mapping  -->  incidents : incident_id
