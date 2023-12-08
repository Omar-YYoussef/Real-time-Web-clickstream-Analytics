#       """ avg_duration_per_page"""

CREATE TABLE avg_duration_per_page(
    Page_URL VARCHAR(260) PRIMARY KEY,
    Avg_Duration_minutes INT,
    Avg_Duration_seconds INT
);



#       """Page views by country"""

CREATE TABLE CountryCounts (
    Country VARCHAR(255) PRIMARY KEY,
    Count INT
);



#       """count_sessions_per_country"""


CREATE TABLE CountrySessions (
    Country VARCHAR(260),
    SessionCount INT
);



#       """Device type distribution"""


CREATE TABLE DeviceTypeCounts (
    Device_Type VARCHAR(255),
    Count INT
);





#       """Count interaction types"""



CREATE TABLE InteractionTypeCounts (
    InteractionTypeCounts VARCHAR(255),
    Count INT
);


#       """Calculate page visit counts"""

CREATE TABLE Page_URLCounts (
   Page_URL VARCHAR(255),
    Count INT
);






INSERT INTO DeviceTypeCounts (Device_Type, Count) VALUES ("TAB",20)
