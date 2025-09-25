
CREATE TABLE dict_voivodeships_coords(
    name TEXT NOT NULL,
    capital_city TEXT PRIMARY KEY ,
    voivodeship_central_lat NUMERIC(9,6) NOT NULL,
    voivodeship_central_long NUMERIC(9,6) NOT NULL,
    capital_city_lat NUMERIC(9,6) NOT NULL,
    capital_city_long NUMERIC(9,6) NOT NULL,
    cepik_voivodeship_code TEXT
);


INSERT INTO dict_voivodeships_coords VALUES
('Dolnośląskie','Wrocław',51.107900,16.978000,51.063700,16.934000),
('Kujawsko-Pomorskie','Toruń',53.013800,18.598400,53.070000,18.600000),
('Lubelskie','Lublin',51.250000,22.566700,51.273400,22.784400),
('Lubuskie','Zielona Góra',52.400000,15.316700,52.326600,15.102100),
('Łódzkie','Łódź',51.766700,19.466700,51.605900,19.364000),
('Małopolskie','Kraków',50.061900,19.936900,49.949600,20.059300),
('Mazowieckie','Warszawa',52.229800,21.011800,52.482700,21.147100),
('Opolskie','Opole',50.672100,17.925300,50.800000,17.900000),
('Podkarpackie','Rzeszów',50.041200,21.999100,49.950000,22.200000),
('Podlaskie','Białystok',53.132500,23.168800,53.333300,22.933300),
('Pomorskie','Gdańsk',54.352000,18.646600,54.200000,18.150000),
('Śląskie','Katowice',50.264900,19.023800,50.483900,19.037000),
('Świętokrzyskie','Kielce',50.870300,20.627500,50.800000,20.800000),
('Warmińsko-Mazurskie','Olsztyn',53.778400,20.480100,53.867000,20.800000),
('Wielkopolskie','Poznań',52.406400,16.925200,52.333300,17.000000),
('Zachodniopomorskie','Szczecin',53.428900,14.553000,53.250000,15.000000);