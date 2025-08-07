------------ETL ARCHITECTURE-----------------

       
      _____________
      \ S3 BUCKET /   
       \         /
        \_______/
           ||
-------------------------------------------------------STAGING LAYER         
      ______________
     [EXTERNAL STAGE]
      ______________
    
           ||

        ( PIPE )
        (      )
        (      )
        (      )
           ||
       __________
      |          |
      |  TABLE   |
      |__________|
           ||
       ____________
       ____________
       \          /
        \ STREAM /
         \      /
          \    /
           \  / 
            \/
------------||----------------------------------------RAW DATA LAYER
        ************       
        *   TASK   * ------ TASK TAKES DATA FROM STREAM IN SDL AND LOADS DATA INTO TABLE IN RDL:
        *          *    
        ************
             ||
         __________
        |          |
        |  TABLE   |
        |__________|
             ||        
        ____________
        ____________
        \          /
         \ STREAM /
          \      /
           \    /
            \  / 
             \/
-------------||-------------------------------------BUSINESS DATA LAYER  
        ************       
        *   TASK   * ------ TASK TAKES DATA FROM STREAM IN RDL AND LOADS DATA INTO TABLE IN BDL:
        *          *    
        ************
             ||
         __________
        |          |
        |  TABLE   |
        |__________|
             ||   
-----------------------------------------------------ACCESS DATA LAYER (REPORTING LAYER)
         __________
        |          |
        |  VIEWS   |
        |__________|
             || 
           REPORTS
        
        