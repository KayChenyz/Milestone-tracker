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
  ),
latest_prdate as (
SELECT [ProjectSizeId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([UpdateAt] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [UpdateAt] ASC) as PRrnk
      /*Rank checkdate to find latest one and it's MPdate */
      ,CAST([Current_Date] as date) PRdate
      ,DATEADD(DAY,7,CAST(PS.[Current_Date] AS DATE)) PR_CP_deadline
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
  JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] 
  AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
  WHERE PROJECT_CATEGORY != 'OEM Auto' AND TBI.DESC_OF_TASK='Pilot Run Form 178 Submitted'
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),
  PRclose_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS PR_Status
      ,[UpdateUser]AS PR_UpdateUser
      ,[UpdateTime]AS PR_UpdateTime
      ,[T_PID_CkList].[Frequency]
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as PRrnk2
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Pilot Run Kick'
  /* Pilot Run Close Control Plan is MP control plan*/
  ),

latest_mpdate as (
SELECT [ProjectId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([CheckDate] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by CheckDate DESC) as MPrnk 
      /*Rank checkdate to find latest one and it's MPdate */
      ,CAST([ChangedDate] as date) MPdate
      ,DATEADD(DAY, -7, CAST([ChangedDate] as date)) MP_CP_deadline
      
  FROM [PR_WEB2].[dbo].[T_MpDate_Log] mplog
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_ID] = mplog.[ProjectId]
  WHERE PROJECT_CATEGORY != 'OEM Auto'
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),
  MPclose_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS MP_Status
      ,[UpdateUser]AS MP_UpdateUser
      ,[UpdateTime]AS MP_UpdateTime
      ,[T_PID_CkList].[Frequency]
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as MRrnk2
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Pilot Run Close'
  /* Pilot Run Close Control Plan is MP control plan*/
  )
  
  SELECT DISTINCT
  [latest_mpdate].[PROJECT_SIZE_ID],
  [latest_mpdate].[PROJECT_SIZE_NAME],
  [MPdate],
  [PRdate],
  [FPdate],
  [PR_CP_deadline],
  [FP_CP_deadline],
  [MP_CP_deadline],
  [MP_Status],
  [PR_Status],
  [FP_Status],
  [MP_UpdateTime],
  [PR_UpdateTime],
  [FP_UpdateTime],
  [MP_UpdateUser], 
  [PR_UpdateUser],
  [FP_UpdateUser]
  /* Drop "rnk" and "ChangeDate"*/
  FROM latest_mpdate
  JOIN MPclose_cp on MPclose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  JOIN latest_prdate ON latest_prdate.[ProjectSizeId]= latest_mpdate.PROJECT_SIZE_ID
  JOIN PRclose_cp on PRclose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  JOIN latest_fpdate ON latest_fpdate.[ProjectSizeId]= latest_mpdate.PROJECT_SIZE_ID
  JOIN FPclose_cp on FPclose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  WHERE MPrnk = 1 AND MRrnk2=1 AND FPrnk1=1 AND FPrnk2=1 AND PRrnk=1 AND PRrnk2=1 AND MPdate > '2023-08-01'
  AND latest_mpdate.PROJECT_SIZE_ID not in (1166, 1109, 1122, 1124, 1125)
  /*Drop duplicated Descent T2, Tacx NEO 3M*, GPSMAP 9000_Black Box*/
  ORDER BY PROJECT_SIZE_ID