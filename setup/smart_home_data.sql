CREATE TABLE IF NOT EXISTS "iot"."smart_home_data" (
   "time" INTEGER,
   "use_kw" DOUBLE PRECISION,
   "gen_kw" DOUBLE PRECISION,
   "house_overall" DOUBLE PRECISION,
   "dishwasher" DOUBLE PRECISION,
   "home_office" DOUBLE PRECISION,
   "fridge" DOUBLE PRECISION,
   "wine_cellar" DOUBLE PRECISION,
   "garage_door" DOUBLE PRECISION,
   "kitchen" DOUBLE PRECISION,
   "barn" DOUBLE PRECISION,
   "well" DOUBLE PRECISION,
   "microwave" DOUBLE PRECISION,
   "living_room" DOUBLE PRECISION,
   "temperature" DOUBLE PRECISION,
   "humidity" DOUBLE PRECISION,
   PRIMARY KEY ("time")
);

CREATE TABLE IF NOT EXISTS "iot"."smart_home_data_temp" (
   "time" INTEGER,
   "use_kw" DOUBLE PRECISION,
   "gen_kw" DOUBLE PRECISION,
   "house_overall" DOUBLE PRECISION,
   "dishwasher" DOUBLE PRECISION,
   "home_office" DOUBLE PRECISION,
   "fridge" DOUBLE PRECISION,
   "wine_cellar" DOUBLE PRECISION,
   "garage_door" DOUBLE PRECISION,
   "kitchen" DOUBLE PRECISION,
   "barn" DOUBLE PRECISION,
   "well" DOUBLE PRECISION,
   "microwave" DOUBLE PRECISION,
   "living_room" DOUBLE PRECISION,
   "temperature" DOUBLE PRECISION,
   "humidity" DOUBLE PRECISION,
   PRIMARY KEY ("time")
);