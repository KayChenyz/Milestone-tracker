with latest_fpdate as (
SELECT [ProjectSizeId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([UpdateAt] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [UpdateAt] DESC) as FPrnk1
      /*Rank checkdate to find latest one and it's MPdate */
      ,CAST([Current_Date] as date) FPdate
      ,DATEADD(DAY,7,CAST(PS.[Current_Date] AS DATE)) FP_CP_deadline
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
  JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] 
  AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
  WHERE PROJECT_CATEGORY != 'OEM Auto' AND TBI.DESC_OF_TASK='Final Factory Form 178 Submitted'
  /*AND PS.ProjectSizeId=1145
   Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),
  FPclose_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS FP_Status
      ,[UpdateUser] AS FP_UpdateUser
      ,[UpdateTime]AS FP_UpdateTime
      ,[T_PID_CkList].[Frequency]
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as FPrnk2
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Factory Prototype Kick'
  /* Pilot Run Close Control Plan is MP control plan*/
  )
  
  SELECT DISTINCT
  [latest_fpdate].[PROJECT_SIZE_ID],
  [PROJECT_SIZE_NAME],
  [FPdate],
  [FP_CP_deadline],
  [FP_Status],
  [FP_UpdateTime],
  [FP_UpdateUser]
  /* Drop "rnk" and "ChangeDate"*/
  FROM latest_fpdate
  JOIN FPclose_cp on FPclose_cp.PROJECT_SIZE_ID = latest_fpdate.PROJECT_SIZE_ID
  WHERE FPrnk1 = 1
  and FPrnk2 = 1
  AND FPdate > '2023-06-01'
  AND latest_fpdate.PROJECT_SIZE_ID not in (1166, 1109, 1122, 1124, 1125)

  /*Drop duplicated Descent T2, Tacx NEO 3M*, GPSMAP 9000_Black Box*/
  ORDER BY FPdate
