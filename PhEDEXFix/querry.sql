SELECT lfn FROM dbsbuffer_file WHERE in_phedex=0 AND (lfn NOT LIKE '%unmerged%' AND lfn NOT LIKE 'MCFakeFile%');
