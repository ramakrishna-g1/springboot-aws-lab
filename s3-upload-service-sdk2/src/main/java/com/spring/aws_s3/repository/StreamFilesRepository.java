package com.spring.aws_s3.repository;

import com.spring.aws_s3.model.UploadStreamTracker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface StreamFilesRepository extends JpaRepository<UploadStreamTracker, String> {

    @Transactional
    @Modifying
    @Query("update UploadStreamTracker s set s.status = :status1 where s.status <> :status2 and s.fileId like concat('%', :runID, '%')")
    void updateStatusByStatusAndFileIdLike(@Param("status1") String status1, @Param("status2") String status2, @Param("runID") String runID);

    @Transactional
    @Modifying
    @Query("update UploadStreamTracker s set s.status = :status1 where s.status <> :status2 and s.fileId in :runIDs")
    void updateStatusByStatusAndFileIdIn(@Param("status1") String status1, @Param("status2") String status2, @Param("runIDs") List<String> runIDs);

    @Transactional
    @Modifying
    @Query("update UploadStreamTracker s set s.status = :status where s.id = :id and s.fileId = :fileId")
    void updateStatusByIdAndFileId(@Param("status") String status, @Param("id") int id, @Param("fileId") String fileId);

    @Transactional
    @Modifying
    @Query("update UploadStreamTracker u set u.retriesDone = :retriesDone where u.id = :id and u.fileId = :fileId")
    void updateRetriesDoneByIdAndFileId(@Param("retriesDone") int retriesDone, @Param("id") int id, @Param("fileId") String fileId);
}
