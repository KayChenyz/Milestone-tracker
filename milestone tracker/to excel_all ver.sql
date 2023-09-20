with latest_fpdate as (
SELECT [ProjectSizeId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([UpdateAt] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [UpdateAt] DESC) as FPrnk1
      /*Rank checkdate to find oldest one and it's FPdate */
      ,CAST([Current_Date] as date) FPdate
      ,DATEADD(DAY,7,CAST(PS.[Current_Date] AS DATE)) FP_CP_deadline
      /*FP_CP_deadline is 7 days after checkdate*/
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
  JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] 
  AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
  WHERE /*PROJECT_CATEGORY != 'OEM Auto' AND*/ TBI.DESC_OF_TASK='Final Factory Form 178 Submitted'
  /*AND PS.ProjectSizeId=1145
   Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),
/*FPKick_real is for real Kick-off meeting time instead of predict one*/
  FPKick_real as(
    SELECT
    [ProjectSizeId]
    ,CAST([Current_Date]as date)as FP_realKick
    ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [Current_Date] ASC) as FPrnk3
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule]
  left join [PR_WEB2].[dbo].[T_GT_TaskBaseInfo]
  on [T_GT_ProjectSchedule].[TASK_ID]=[T_GT_TaskBaseInfo].[TASK_ID]
  left join [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo]
  on ProjectSizeId=[T_PS_ProdSizeRelateInfo].PROJECT_SIZE_ID
  where DESC_OF_TASK='Factory Prototype Kick off Meeting'
  ),

  FPKick_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS FP_Status
      ,[T_PID_CkList].[Frequency]
      ,NULLIF(LEFT(UpdateUser, CHARINDEX('@', UpdateUser + '@') - 1), '')AS fp_updater
      /*replace''to null to correspond the case-then syntax below*/
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as FPrnk2
      /*Rank checkdate to find oldest one and it's FPdate */
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Factory Prototype Kick'
  /* Factory Prototype Kick Control Plan is FP control plan*/
  ),
latest_wpdate as (
 SELECT [ProjectSizeId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([UpdateAt] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [UpdateAt] ASC) as WPrnk
      /*Rank checkdate to find oldest one and it's PRdate */
      ,CAST([Current_Date] as date) WPdate
      ,DATEADD(DAY,42,CAST(PS.[Current_Date] AS DATE)) WP_CP_deadline
      /*PR_CP_deadline is 7 days after checkdate*/
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
  JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] 
  AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
  WHERE /*PROJECT_CATEGORY != 'OEM Auto' AND */TBI.DESC_OF_TASK='Working Prototype Form 178 Submitted'
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),

  WPClose_real as(
    SELECT
    [ProjectSizeId]
    ,CAST([Current_Date]as date)as WP_realClose
    ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [Current_Date] ASC) as WPrnk3
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule]
  left join [PR_WEB2].[dbo].[T_GT_TaskBaseInfo]
  on [T_GT_ProjectSchedule].[TASK_ID]=[T_GT_TaskBaseInfo].[TASK_ID]
  left join [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo]
  on ProjectSizeId=[T_PS_ProdSizeRelateInfo].PROJECT_SIZE_ID
  where DESC_OF_TASK='Detail Design Close Meeting'
  ),

  WPClose_cp as(
    SELECT 
    [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS WP_Status
      ,[T_PID_CkList].[Frequency]
      ,NULLIF(LEFT(UpdateUser, CHARINDEX('@', UpdateUser + '@') - 1), '') AS wp_updater
      /*replace''to null to correspond the case-then syntax below*/
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as WPrnk2
      /*Rank checkdate to find oldest one and it's PRdate */
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'DFM review' and Category_Name = 'Detail Design Close'
   /* Pilot Run Kick Control Plan is PR control plan*/
  ),


latest_prdate as (
SELECT [ProjectSizeId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,CAST([UpdateAt] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [UpdateAt] ASC) as PRrnk
      /*Rank checkdate to find oldest one and it's PRdate */
      ,CAST([Current_Date] as date) PRdate
      ,DATEADD(DAY,7,CAST(PS.[Current_Date] AS DATE)) PR_CP_deadline
      /*PR_CP_deadline is 7 days after checkdate*/
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
  JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] 
  AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
  WHERE /*PROJECT_CATEGORY != 'OEM Auto' AND */TBI.DESC_OF_TASK='Pilot Run Form 178 Submitted'
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),

  PRKick_real as(
    SELECT
    [ProjectSizeId]
    ,CAST([Current_Date]as date)as PR_realKick
    ,RANK() OVER (PARTITION by ProjectSizeId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by [Current_Date] ASC) as PRrnk3
  FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule]
  left join [PR_WEB2].[dbo].[T_GT_TaskBaseInfo]
  on [T_GT_ProjectSchedule].[TASK_ID]=[T_GT_TaskBaseInfo].[TASK_ID]
  left join [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo]
  on ProjectSizeId=[T_PS_ProdSizeRelateInfo].PROJECT_SIZE_ID
  where DESC_OF_TASK='Pilot Run Kick off Meeting'
  ),

  PRKick_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS PR_Status
      ,[T_PID_CkList].[Frequency]
      ,NULLIF(LEFT(UpdateUser, CHARINDEX('@', UpdateUser + '@') - 1), '') AS pr_updater
      /*replace''to null to correspond the case-then syntax below*/
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as PRrnk2
      /*Rank checkdate to find oldest one and it's PRdate */
  FROM [PR_WEB2].[dbo].[T_PID_CkList]
  LEFT JOIN [T_PID_CkListFiles]
  ON ([T_PID_CkListFiles].[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
  and [T_PID_CkListFiles].[MpType_ID] = [T_PID_CkList].[MpType_ID]
  and [T_PID_CkListFiles].[Category_ID] = [T_PID_CkList].[Category_ID]
  and [T_PID_CkListFiles].[Item_ID] = [T_PID_CkList].[Item_ID]
  and [T_PID_CkListFiles].[Frequency] = [T_PID_CkList].[Frequency]
  /* Frequency is duplicated phase count (e.g. Twice FP) */)
  where Item_Name = 'Control Plan' and Category_Name = 'Pilot Run Kick'
  /* Pilot Run Kick Control Plan is PR control plan*/
  ),

latest_mpdate as (
SELECT [ProjectId]
      ,[PROJECT_SIZE_ID]
      ,[PROJECT_SIZE_NAME]
      ,[PROJECT_CATEGORY]
      ,CAST([CheckDate] as date) as ChangeDate
      ,RANK() OVER (PARTITION by ProjectId,PROJECT_SIZE_ID,PROJECT_SIZE_NAME ORDER by CheckDate DESC) as MPrnk 
      /*Rank checkdate to find oldest one and it's MPdate */
      ,CAST([ChangedDate] as date) MPdate
      ,DATEADD(DAY, -7, CAST([ChangedDate] as date)) MP_CP_deadline
      /*MP_CP_deadline is 7 days before checkdate*/
  FROM [PR_WEB2].[dbo].[T_MpDate_Log] mplog
  JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] info
  ON info.[PROJECT_ID] = mplog.[ProjectId]
  /* Since rnk can't be "whered" in above query, so I create a CTE to store query result temporary and do "where" below */
  ),

  MPclose_cp as(
    SELECT [T_PID_CkList].[PROJECT_SIZE_ID]
      ,[Category_Name]
      ,[Item_Name]
      ,[Status] AS MP_Status
      ,[T_PID_CkList].[Frequency]
      ,NULLIF(LEFT(UpdateUser, CHARINDEX('@', UpdateUser + '@') - 1), '')AS mp_updater
      /*replace''to null to correspond the case-then syntax below*/
      ,RANK() OVER (PARTITION by [T_PID_CkList].[PROJECT_SIZE_ID],[Category_Name],[Item_Name]ORDER by [T_PID_CkList].[Frequency] ASC) as MRrnk2
      /*Rank checkdate to find oldest one and it's MPdate */
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
  /* Final select form six CTE above*/
  SELECT DISTINCT
  [PROJECT_CATEGORY] as Segement,
  [latest_mpdate].[PROJECT_SIZE_NAME] as Project,
  [latest_mpdate].[PROJECT_SIZE_ID],

  CASE    
       WHEN [WP_CP_deadline] IS NULL THEN '--'
       WHEN [FP_CP_deadline] IS NULL THEN '--'/*if the date haven't update then show--*/
       WHEN [PR_CP_deadline] IS NULL THEN '--'
       WHEN [MP_CP_deadline] IS NULL THEN '--'
       WHEN GETDATE() BETWEEN [WP_CP_deadline] AND [FP_CP_deadline] THEN 'WP_CP'
       WHEN GETDATE() BETWEEN [FP_CP_deadline] AND [PR_CP_deadline] THEN 'PR_CP'
       WHEN GETDATE() BETWEEN [PR_CP_deadline] AND [MP_CP_deadline] THEN 'MP_CP'
       WHEN GETDATE() < [FP_CP_deadline] THEN 'FP_CP'
       WHEN GETDATE() > [MP_CP_deadline] THEN 'Finished'
      ELSE 'error'
  END AS Upcoming_Status,/* decide the upcoming status*/

  CASE 
       WHEN [WP_realClose] IS NULL THEN [WP_CP_deadline]
       WHEN [WP_realClose] IS NOT NULL THEN [WP_realClose]
      ELSE 'error'
  END AS WP_Close,/* decide the upcoming status*/

  CASE 
       WHEN [FP_realKick] IS NULL THEN [FP_CP_deadline]
       WHEN [FP_realKick] IS NOT NULL THEN [FP_realKick]
      ELSE 'error'
  END AS FP_Kick,/* decide the upcoming status*/

  CASE 
       WHEN [PR_realKick] IS NULL THEN [PR_CP_deadline]
       WHEN [PR_realKick] IS NOT NULL THEN [PR_realKick]
      ELSE 'error'
  END AS PR_Kick,


  [MP_CP_deadline] as MP_Kick


  /* Drop "rnk" and "ChangeDate"*/
  FROM latest_mpdate
  JOIN MPclose_cp on MPclose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  
  JOIN latest_wpdate ON latest_wpdate.[ProjectSizeId]= latest_mpdate.PROJECT_SIZE_ID
  JOIN WPClose_cp on WPClose_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  JOIN WPClose_real on WPClose_real.ProjectSizeId = latest_mpdate.PROJECT_SIZE_ID
  
  JOIN latest_fpdate ON latest_fpdate.[ProjectSizeId]= latest_mpdate.PROJECT_SIZE_ID
  JOIN FPKick_cp on FPKick_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  JOIN FPKick_real on FPKick_real.ProjectSizeId = latest_mpdate.PROJECT_SIZE_ID
  
  JOIN latest_prdate ON latest_prdate.[ProjectSizeId]= latest_mpdate.PROJECT_SIZE_ID
  JOIN PRKick_cp on PRKick_cp.PROJECT_SIZE_ID = latest_mpdate.PROJECT_SIZE_ID
  JOIN PRKick_real on PRKick_real.ProjectSizeId= latest_mpdate.PROJECT_SIZE_ID
  
  
  WHERE MPrnk = 1 AND MRrnk2=1 
  AND FPrnk1=1 AND FPrnk2=1 AND FPrnk3=1 
  AND PRrnk=1 AND PRrnk2=1  AND PRrnk3=1 
  AND WPrnk=1 and WPrnk2=1 and WPrnk3=1
  /*only show the first FP,PR and MP*/
  AND MP_CP_deadline > DATEADD(MONTH, -1, GETDATE())
  /*only show the project in tje future (one month is for buffer)*/
  AND latest_mpdate.PROJECT_SIZE_ID not in (1166, 1109, 1122, 1124, 1125)
  /*Drop duplicated Descent T2, Tacx NEO 3M*, GPSMAP 9000_Black Box*/

  ORDER BY FP_Kick