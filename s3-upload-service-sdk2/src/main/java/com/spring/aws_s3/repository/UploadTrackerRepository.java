package com.spring.aws_s3.repository;

import com.spring.aws_s3.model.UploadTracker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface UploadTrackerRepository extends JpaRepository<UploadTracker, Long> {
    @Transactional
    @Modifying
    @Query("update UploadTracker u set u.fileStatus = :fileStatus " +
            "where u.fileName = :fileName and u.filePath = :filePath")
    int updateFileStatusByFileNameAndFilePath(@Param("fileStatus") String fileStatus,
                                              @Param("fileName") String fileName, @Param("filePath") String filePath);

    @Transactional
    @Modifying
    @Query("update UploadTracker u set u.retriesDone = :retriesDone " +
            "where u.fileName = :fileName and u.filePath = :filePath")
    int updateRetriesDoneByFileNameAndFilePath(@Param("retriesDone") int retriesDone,
                                               @Param("fileName") String fileName, @Param("filePath") String filePath);
}