create or replace procedure species_json (v_result OUT clob )
  is
  vcronjob                           VARCHAR2(150)  := 'ON DEMAND';
 	    vbatchprocess                  VARCHAR2 (150)  := NULL;
	    vmodulename                    varchar2 (150)  := 'VTR_JSON'; 
		vprocedurename                 VARCHAR2 (255)  := 'SPECIES_JSON';
		vtablename                     VARCHAR2 (50)   := '';	    
		ilogid                         INT             := 0;
		VSQL                           varchar2 (4000);		
        errmsg           VARCHAR2 (2000);
        sql_stmt             VARCHAR2 (2000);
        vowner           VARCHAR2 (50)   :='CFDRS';
        ifsoseq          NUMBER          :=0;
        v_compliance_start  date;
        v_compliance_end    date;
        v_name             VARCHAR2 (25);
        v_debug  NUMBER;
        v_loop  NUMBER;
        v_maxdate date;
        v_exclude_rpt_type NUMBER := 0;
        x_exist NUMBER := 0;    -- Flag indicating whether a species is excluded from some report types
        v_last_sppcode VARCHAR2 (7)  := '';

        ITERATION_LENGTH CONSTANT NUMBER := 4000;

        SPPSYN CONSTANT VARCHAR2(6) := 'SPPSYN';
        SPPCODE CONSTANT VARCHAR2(7) := 'SPPCODE';
        SPPNAME CONSTANT VARCHAR2(7) := 'SPPNAME';
        ITIS CONSTANT VARCHAR2(4) := 'ITIS';
        X_RPT_TYPE CONSTANT VARCHAR2(10) := 'X_RPT_TYPE';

        species_object  JSON_OBJECT_T := json_object_t();
        species_array  JSON_ARRAY_T := json_array_t();
        exclude_array  JSON_ARRAY_T := json_array_t();
        empty_array  JSON_ARRAY_T := json_array_t();
  begin
    DBMS_OUTPUT.ENABLE(1000000);

    for s_data in (with species_data as (SELECT spp.sppcode sppsyn,
        spp.sppcode,
        spp.sppname,
        nvl(FMP_MGMT.GET_SPECIES_ITIS_FNC('SPPCODE', spp.sppcode), 0) ||
        CASE
        WHEN row_number() OVER (PARTITION BY nvl(FMP_MGMT.GET_SPECIES_ITIS_FNC('SPPCODE', spp.sppcode), 0) order by spp.sppcode)
        > 1
        THEN '-'||
        row_number() OVER (PARTITION BY nvl(FMP_MGMT.GET_SPECIES_ITIS_FNC('SPPCODE', spp.sppcode), 0) order by spp.sppcode)
        ELSE ''
        END itis,
        spp.required_uom,
        ve.exclude_rpt_type
        FROM vtr.vlspptbl spp
        LEFT OUTER JOIN vtr.vlspecies_exclude ve
        ON ve.sppcode = spp.sppcode
        WHERE spp.public_access = 'Y'
        AND (FMP_MGMT.GET_SPECIES_ITIS_FNC('SPPCODE', spp.sppcode) is not null OR spp.sppcode = 'NC')
        ORDER BY spp.sppcode ASC)
    select s.sppsyn, s.sppcode, s.sppname, s.itis, s.exclude_rpt_type
    from species_data s) loop
    
        IF (s_data.exclude_rpt_type IS NOT NULL) THEN
            exclude_array.append(s_data.exclude_rpt_type);
        END IF;

        IF (s_data.sppsyn = v_last_sppcode) THEN
            IF (s_data.exclude_rpt_type IS NOT NULL) THEN
                species_object.remove(X_RPT_TYPE);
                species_object.put(X_RPT_TYPE, exclude_array);
            END IF;
        ELSE
            species_array.append(species_object);
                    --Start cleaning JSON object
            species_object.remove(SPPSYN);
            species_object.remove(SPPCODE);
            species_object.remove(SPPNAME);
            species_object.remove(ITIS);
            species_object.remove(X_RPT_TYPE);
            exclude_array := json_array_t();    -- resetting the excluded types array
            
            species_object.put(SPPSYN, s_data.sppsyn);
            species_object.put(SPPCODE, s_data.sppcode);
            species_object.put(SPPNAME, s_data.sppname);
            species_object.put(ITIS, s_data.itis);
/*
        SELECT count(*) INTO x_exist FROM vtr.vlspecies_exclude
        WHERE sppcode = s_data.sppcode;

        IF x_exist > 0 THEN
            for x_data in (with exclude_data as (select distinct exclude_rpt_type
                FROM vtr.vlspecies_exclude
                WHERE sppcode = s_data.sppcode)
            select x.exclude_rpt_type
            from exclude_data x) loop

                exclude_array.append(x_data.exclude_rpt_type);
            end loop;
            species_object.put(X_RPT_TYPE, exclude_array);
            exclude_array := json_array_t();    -- resetting the array
        END IF;
        */
  /*
         IF ( isi_CurrentDebugLevel >= VERBOSE_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE('Species code:' || species_object.to_clob());
            DBMS_OUTPUT.PUT_LINE(':'||s_data.sppcode||':');
         END IF;
    */
        END IF;

        v_last_sppcode := s_data.sppsyn;
    end loop;

     --IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
        DBMS_OUTPUT.PUT_LINE('Species JSON:' || species_array.to_clob());
     --END IF;

    v_result := species_array.to_clob();

  --fso_admin.log_event (vbatchprocess, vmodulename, vprocedurename, ifsoseq, 'SUCCESSFUL', 'Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);
  EXCEPTION
    WHEN OTHERS THEN
        errmsg := errmsg || ' SQL Error on ' || vtablename ||' : ' || SQLERRM;

        v_result := empty_array.to_clob();
        --IF ( isi_CurrentDebugLevel >= ERROR_DEBUG_LEVEL ) THEN
            DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
        --END IF;
  end;