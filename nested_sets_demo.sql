-- Consider using nested sets. This gets a bit involved, but is 
-- crazy powerful. Tool up your table for nested sets with integer columns named
-- left and right. I abbreviate a bit so it's clear that references to them are
-- not in fact parts of join predicates.

DECLARE @tree table(
	  id					int PRIMARY KEY
	, parent_id				int 
	, node_name				varchar(50)
	-- add these columns to facilitate nested sets exercise
	, lvl					int
	, lft					int
	, rgt					int
	, count_children		int
	, count_descendants		int
	, bread_crumb_trail		varchar(60) 
)

DECLARE @x table ( -- scratch table for nested set calcs
	  id					int PRIMARY KEY
	, parent_left			int
	, descendants			int
	, lft					int
	, rgt					int
	, ord					int
)

DECLARE 
	  @max_level			int
	, @row_id				int
	, @level				int
	, @count_rows			int
	, @last_parent_left		int
	, @fetchStatus			int = 0
	, @parent_left			int 
	, @lft					int
	, @rgt					int
	, @last_right			int
	, @count_descendants	int
	, @guilty_node_id		int

INSERT @tree (id, parent_id, node_name)
VALUES
	  (1	, 9		, 'Volkswagen' )
	, (2	, 8		, 'Slartibartfast' )
	, (3	, 8		, 'Arthur Dent' )
	, (4	, 8		, 'Trillian' )
	, (5	, 9		, 'Subaru' )
	, (11	, 5		, 'Crosstrek' )
	, (50	, 11	, 'Base' )
	, (51	, 11	, 'Premium' )
	, (52	, 11	, 'Sport' )
	, (53	, 11	, 'Limited' )
	, (54	, 11	, 'Wilderness' )
	, (12	, 5		, 'Outback' )
	, (6	, 22	, 'African Savanna' )
	, (7	, 22	, 'African Forest' )
	, (500	, 22	, 'Asian' )
	, (8	, NULL	, 'Hitchhikers' )
	, (9	, NULL	, 'Cars')
	, (22	, NULL	, 'Elephants')


-- count children, mark level 0 (zero) 
UPDATE p
SET 
	  -- nodes with NULL parent_id are level zero (0) [roots], movie stars
	  lvl = CASE WHEN p.parent_id IS NULL THEN 0 ELSE NULL END 
	, count_children = ISNULL(cc.count_children, 0)
	, bread_crumb_trail = p.node_name
FROM @tree as p
LEFT OUTER JOIN (
	SELECT parent_id, count(*) as count_children
	FROM @tree as c
	GROUP BY c.parent_id
) as cc
	ON p.id = cc.parent_id


-- mark descendant levels  
WHILE EXISTS (
	SELECT TOP 1 1
	FROM @tree as c
	WHERE c.lvl IS NULL
	AND EXISTS (
		SELECT TOP 1 1
		FROM @tree as p
		WHERE c.parent_id = p.id
	)
)
BEGIN
	UPDATE c
	SET 
		  lvl = p.lvl + 1
		, bread_crumb_trail = p.bread_crumb_trail + ' -> ' + c.node_name
	FROM @tree as c
	INNER JOIN @tree as p 
		ON c.parent_id = p.id
	WHERE c.lvl IS NULL
	AND p.lvl IS NOT NULL
END


SELECT TOP 1 @max_level = lvl
FROM @tree ORDER BY lvl DESC;

SELECT @level = @max_level;

WHILE @level >= -1
BEGIN
	PRINT @level;

	UPDATE p
	SET 
		  count_descendants = ISNULL(p.count_children, 0) + ISNULL(cc.count_descendants, 0)
	FROM @tree AS p
	LEFT OUTER JOIN (
		SELECT 
			  b.parent_id
			, SUM(count_descendants) AS count_descendants
		FROM @tree AS b  
		WHERE b.lvl = @level + 1
		GROUP BY b.parent_id
	) AS cc
		ON p.id = cc.parent_id
	WHERE p.lvl = @level

	SELECT @count_Rows = @@ROWCOUNT;

	SELECT @level -= 1
END




SELECT @level = -1

WHILE @level <= @max_level
BEGIN
	SELECT @last_parent_left = -1, @fetchStatus = 0;

	DELETE @x;

	INSERT @x (
		id, parent_left, descendants, ord
	)
	SELECT 
			b.id
		, ISNULL(p.lft, 0) AS parent_left
		, b.count_descendants 
		, ROW_NUMBER() OVER (PARTITION BY p.lft ORDER BY b.node_name) as ord
	FROM @tree AS b  
	LEFT OUTER JOIN @tree AS p  
		ON b.parent_id = p.id
		AND p.lvl = b.lvl - 1
	WHERE b.lvl = @level

	DECLARE curs_x CURSOR FOR
	SELECT id, parent_left, descendants 
	FROM @x
	ORDER BY parent_left, ord 
	FOR UPDATE OF lft, rgt 

	OPEN curs_x

	SELECT @fetchStatus = 0;

	WHILE @fetchStatus = 0
	BEGIN
		FETCH NEXT FROM curs_x
		INTO @row_id, @parent_left, @count_descendants 

		SELECT @fetchStatus = @@FETCH_STATUS;

		IF @fetchStatus = 0
		BEGIN
			IF @parent_left = @last_parent_left
			BEGIN
				SELECT 
					  @lft = @last_right + 1
					, @rgt = @lft + @count_descendants * 2 + 1
					, @last_right = @rgt
			END

			ELSE
			BEGIN
				SELECT 
					  @last_parent_left = @parent_left
					, @lft = @parent_left + 1
					, @rgt = @lft + @count_descendants * 2 + 1
					, @last_right = @rgt
			END

			UPDATE @x
			SET 
				  lft = @lft
				, rgt = @rgt
			WHERE id = @row_id
		END
	END

	CLOSE curs_x
	DEALLOCATE curs_x

	UPDATE h
	SET 
		  lft = x.lft
		, rgt = x.rgt 
		, bread_crumb_trail = bread_crumb_trail
	FROM @tree as h
	INNER JOIN @x as x
		ON h.id = x.id

	SELECT @level += 1;
END

-- The @tree table is set up for reporting using nested sets. The maintenance of the left/right values 
-- isn't cheap. But if you're hierarchical dimension data isn't in rapid flux, it's worthwhile to use it 
-- and include a refresh of the nested set metadata as part of a maintenance procedure. Once the provisions 
-- are in, queries against it are powerful and fast.

-- Present contents of the entire hierarchical table as if it was a fully exploded tree
SELECT t.id, t.bread_crumb_trail, REPLICATE('|   ', t.lvl) + t.node_name as node_text
FROM @tree as t
ORDER BY lft

/*
-- result
id     bread_crumb_trail                                          node_text
------ -------------------------------------------- --------------------------
9      Cars                                         Cars
5      Cars -> Subaru                               |   Subaru
11     Cars -> Subaru -> Crosstrek                  |   |   Crosstrek
50     Cars -> Subaru -> Crosstrek -> Base          |   |   |   Base
53     Cars -> Subaru -> Crosstrek -> Limited       |   |   |   Limited
51     Cars -> Subaru -> Crosstrek -> Premium       |   |   |   Premium
52     Cars -> Subaru -> Crosstrek -> Sport         |   |   |   Sport
54     Cars -> Subaru -> Crosstrek -> Wilderness    |   |   |   Wilderness
12     Cars -> Subaru -> Outback                    |   |   Outback
1      Cars -> Volkswagen                           |   Volkswagen
22     Elephants                                    Elephants
7      Elephants -> African Forest                  |   African Forest
6      Elephants -> African Savanna                 |   African Savanna
500    Elephants -> Asian                           |   Asian
8      Hitchhiker                                   Hitchhiker
3      Hitchhiker -> Arthur Dent                    |   Arthur Dent
2      Hitchhiker -> Slartibartfast                 |   Slartibartfast
4      Hitchhiker -> Trillian                       |   Trillian

*/



-- descendants of a guilty node
SELECT @guilty_node_id = 11 -- Crosstrek

SELECT t.id, REPLICATE('|   ', t.lvl - gn.glvl) + t.node_name as node_text
FROM @tree as t
CROSS JOIN (
	SELECT 
		  lft as glft
		, rgt as grgt
		, lvl as glvl
	FROM @tree as t
	WHERE t.id = @guilty_node_id
) as gn 
WHERE t.lft >= gn.glft	
AND t.rgt <= gn.grgt	
-- AND t.lvl > gn.glvl -- un-comment if you don't want root included
ORDER BY t.lft

/*
-- result
id          node_text
----------- -------------------
11          Crosstrek
50          |   Base
51          |   Premium
52          |   Sport
53          |   Limited
54          |   Wilderness

*/



-- ancestors of a guilty node
SELECT @guilty_node_id = 54 -- Wilderness

SELECT t.id, REPLICATE('|   ', t.lvl) + t.node_name as node_text
FROM @tree as t
CROSS JOIN (
	SELECT 
		  lft as glft
		, rgt as grgt
		, lvl as glvl
	FROM @tree as t
	WHERE t.id = @guilty_node_id
) as gn 
WHERE t.lft <= gn.glft	
AND t.rgt >= gn.grgt	
-- AND t.lvl > gn.glvl -- un-comment if you don't want root included
ORDER BY t.lft DESC

/*
-- result
id          node_text
----------- ---------------------------------
9           Cars
5           |   Subaru
11          |   |   Crosstrek
54          |   |   |   Wilderness

*/



