class SqlQueries:
    immigration_table_insert = ("""
        SELECT 
            admnum, 
            cicid, 
            NVL(cicid, '0') || '-' || admnum, 
            cicid || '-' || NVL(fltno, '00') || '-' || admnum, 
            admnum || '-' || cicid || '-' || NVL(dtaddto, '-00'), 
            i94yr, 
            i94mon, 
            CAST(SUBSTRING(dtadfile, 7, 2) AS BIGINT), 
            i94mode, 
            "count" 
        FROM public.staging_immigration
    """)

    passenger_table_insert = ("""
        SELECT 
            NVL(cicid, '0') || '-' || admnum, 
            insnum, 
            gender, 
            i94bir, 
            biryear, 
            occup, 
            i94addr, 
            s.state_of_residence 
         FROM  public.staging_immigration im 
         LEFT JOIN public.states s ON im.i94addr = s.key
    """)

    flight_table_insert = ("""
        SELECT 
            cicid || '-' || NVL(fltno, '00') || '-' || admnum, 
            fltno, 
            i94cit, 
            cc.country, 
            i94res, 
            cc2.country, 
            i94port, 
            ac.airport_city, 
            i94addr, 
            depdate, 
            arrdate 
        FROM public.staging_immigration im 
        LEFT JOIN public.country_codes cc ON im.i94cit = cc.key 
        LEFT JOIN public.country_codes cc2 ON im.i94cit = cc2.key 
        LEFT JOIN public.airport_city ac ON im.i94port = ac.key
    """)

    flight_flag_table_insert = ("""
        SELECT 
            cicid, 
            admnum, 
            entdepa, 
            entdepd, 
            entdepu, 
            matflag 
        FROM public.staging_immigration
    """)

    visa_table_insert = ("""
        SELECT 
            admnum || '-' || cicid || '-' || NVL(dtaddto, '-00'), 
            i94visa, 
            vt.visa_type_class, 
            visatype, 
            visapost, 
            dtaddto 
        FROM  public.staging_immigration im 
        LEFT JOIN public.visa_type vt ON im.i94visa = vt.key
    """)