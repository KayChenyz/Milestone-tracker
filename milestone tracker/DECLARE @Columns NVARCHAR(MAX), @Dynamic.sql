DECLARE @Columns NVARCHAR(MAX), @DynamicSQL NVARCHAR(MAX);

-- 生成需要作为列的 PROJECT_SIZE_NAME
SET @Columns = N'';

-- 生成需要作为列的 PROJECT_SIZE_NAME
SELECT @Columns += QUOTENAME(PROJECT_SIZE_NAME) + N','
FROM (
    SELECT PS.[ProjectSizeId]
        , info.[PROJECT_SIZE_NAME]
    FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
    JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
    JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] AS info ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
    WHERE info.PROJECT_CATEGORY != 'OEM Auto' AND TBI.DESC_OF_TASK='Pilot Run Form 178 Submitted'
        AND PS.[Current_Date] > '2023-06-01'
        AND PS.[ProjectSizeId] NOT IN (1166, 1109, 1122, 1124, 1125)
    GROUP BY PS.[ProjectSizeId], info.[PROJECT_SIZE_NAME]
) AS SubQuery;
SET @Columns = LEFT(@Columns, LEN(@Columns) - 1); -- 去除最后一个逗号

-- 动态构建查询语句
SET @DynamicSQL = N'
SELECT *
FROM (
    SELECT
        latest_prdate.[ProjectSizeId],
        latest_prdate.[PROJECT_SIZE_NAME],
        latest_prdate.[PRdate],
        latest_prdate.[PR_CP_deadline],
        latest_prdate.[Status],
        latest_prdate.[UpdateTime],
        latest_prdate.[UpdateUser]
    FROM (
        SELECT PS.[ProjectSizeId]
            , info.[PROJECT_SIZE_NAME]
            , CAST(PS.[UpdateAt] as date) as ChangeDate
            , CAST(PS.[Current_Date] as date) PRdate
            , DATEADD(DAY,7,CAST(PS.[Current_Date] AS DATE)) PR_CP_deadline
            , PC.[Status]
            , PC.[UpdateTime]
            , PC.[UpdateUser]
        FROM [PR_WEB2].[dbo].[T_GT_ProjectSchedule] AS PS
        JOIN [PR_WEB2].[dbo].[T_GT_TaskBaseInfo] AS TBI ON PS.[TASK_ID]=TBI.[TASK_ID]
        JOIN [PR_WEB2].[dbo].[T_PS_ProdSizeRelateInfo] AS info ON info.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
        LEFT JOIN (
            SELECT info.[PROJECT_SIZE_ID], [Status], [UpdateUser], [UpdateTime]
            FROM [PR_WEB2].[dbo].[T_PID_CkList]
            LEFT JOIN [T_PID_CkListFiles] AS CF ON (
                CF.[PROJECT_SIZE_ID] = [T_PID_CkList].[PROJECT_SIZE_ID]
                AND CF.[MpType_ID] = [T_PID_CkList].[MpType_ID]
                AND CF.[Category_ID] = [T_PID_CkList].[Category_ID]
                AND CF.[Item_ID] = [T_PID_CkList].[Item_ID]
                AND CF.[Frequency] = [T_PID_CkList].[Frequency]
            )
            WHERE [T_PID_CkList].[Item_Name] = ''FRM178 release''
                AND [T_PID_CkList].[Category_Name] = ''Pilot Run Kick''
        ) AS PC ON PC.[PROJECT_SIZE_ID] = PS.[ProjectSizeId]
        WHERE info.PROJECT_CATEGORY != ''OEM Auto'' AND TBI.DESC_OF_TASK=''Pilot Run Form 178 Submitted''
            AND PS.[Current_Date] > ''2023-06-01''
            AND PS.[ProjectSizeId] NOT IN (1166, 1109, 1122, 1124, 1125)
    ) AS latest_prdate
) AS SourceTable
PIVOT (
    MAX([Status]) FOR [PROJECT_SIZE_NAME] IN (' + @Columns + N')
) AS PivotTable
ORDER BY [PRdate];
';

-- 执行动态SQL
EXEC sp_executesql @DynamicSQL;

