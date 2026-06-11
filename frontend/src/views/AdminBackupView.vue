<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { 
  Download, 
  Upload as UploadIcon,
  Warning, 
  Management, 
  CircleCheck, 
  Clock, 
  Cpu, 
  Connection,
  FolderChecked,
  InfoFilled,
  RefreshRight
} from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
const restoreLoading = ref(false)
const lastBackupTime = ref(localStorage.getItem('last_backup_time') || '从未备份')
  
  async function handleBackup() {
    loading.value = true
    try {
      const response = await api.post('/api/admin/backup', {}, {
        responseType: 'blob'
      })
      
      const disposition = response.headers['content-disposition']
      let fileName = `backup_${new Date().toISOString().split('T')[0]}.zip`
      if (disposition && disposition.includes('filename=')) {
        fileName = disposition.split('filename=')[1].replace(/"/g, '')
      }
  
      const url = window.URL.createObjectURL(new Blob([response.data]))
      const link = document.createElement('a')
      link.href = url
      link.setAttribute('download', fileName)
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      
      const now = new Date().toLocaleString()
      lastBackupTime.value = now
      localStorage.setItem('last_backup_time', now)
      
      ElMessage({
        message: '系统全量数据备份已生成',
        type: 'success',
        duration: 5000,
        showClose: true
      })
    } catch (e) {
      console.error('Backup error:', e)
      ElMessage.error('备份生成失败，请检查服务器连接或后端日志')
    } finally {
      loading.value = false
    }
  }
  
  async function handleRestore(file) {
  try {
    await ElMessageBox.confirm(
      '此操作将永久覆盖当前数据库中的所有数据（用户、课程、资源等），且无法撤销。建议在操作前先执行一次导出备份。是否确定继续？',
      '极端高危操作警告',
      {
        confirmButtonText: '确定还原',
        cancelButtonText: '取消',
        type: 'warning',
        confirmButtonClass: 'el-button--danger'
      }
    )
  } catch (e) {
    return
  }

  restoreLoading.value = true
  const formData = new FormData()
  formData.append('file', file.raw)

  try {
    await api.post('/api/admin/restore', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      }
    })
    ElMessage.success('数据恢复成功，系统已回滚至备份状态')
    // 强制刷新页面以确保数据同步
    setTimeout(() => {
      window.location.reload()
    }, 1500)
  } catch (e) {
    console.error('Restore error:', e)
    ElMessage.error(e.response?.data?.error?.message || '恢复失败，请确保备份文件格式正确')
  } finally {
    restoreLoading.value = false
  }
}

const stats = ref([
  { label: 'MySQL 业务库', status: '在线', icon: Cpu, color: '#409EFF' },
  { label: 'Neo4j 知识图谱', status: '同步中', icon: Connection, color: '#67C23A' },
  { label: '本地文件存储', status: '正常', icon: FolderChecked, color: '#E6A23C' }
])
</script>

<template>
  <div class="backup-view">
    <!-- 顶部状态概览 -->
    <el-row :gutter="20" class="status-row">
      <el-col :span="8" v-for="item in stats" :key="item.label">
        <el-card shadow="hover" class="status-mini-card">
          <div class="mini-content">
            <el-icon :size="24" :style="{ color: item.color }"><component :is="item.icon" /></el-icon>
            <div class="info">
              <div class="label">{{ item.label }}</div>
              <div class="status">
                <span class="dot" :style="{ backgroundColor: item.color }"></span>
                {{ item.status }}
              </div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" class="main-action-row">
      <!-- 导出区域 -->
      <el-col :span="12">
        <el-card class="action-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span class="title">全量数据导出</span>
              <el-tag size="small" type="success" effect="plain">推荐操作</el-tag>
            </div>
          </template>

          <div class="backup-hero">
            <div class="hero-icon">
              <el-icon :size="32" color="#409EFF"><Management /></el-icon>
            </div>
            <div class="hero-text">
              <h3>准备好保护您的数据了吗？</h3>
              <p>系统将自动梳理 MySQL 数据库中的所有业务实体，包括用户权限、资源元数据及关联关系。</p>
            </div>
          </div>

          <div class="backup-steps small-steps">
            <div class="step-item">
              <div class="step-num">1</div>
              <div class="step-content">扫描业务表</div>
            </div>
            <div class="step-item">
              <div class="step-num">2</div>
              <div class="step-content">序列化 JSON</div>
            </div>
            <div class="step-item">
              <div class="step-num">3</div>
              <div class="step-content">执行 ZIP 压缩</div>
            </div>
          </div>

          <div class="action-footer compact-footer">
            <div class="last-time">
              <el-icon><Clock /></el-icon>
              上次：<span>{{ lastBackupTime }}</span>
            </div>
            <el-button 
              type="primary" 
              class="backup-btn"
              size="small"
              :loading="loading"
              @click="handleBackup"
            >
              <el-icon v-if="!loading" class="el-icon--left"><Download /></el-icon>
              {{ loading ? '正在打包...' : '立即导出' }}
            </el-button>
          </div>
        </el-card>
      </el-col>

      <!-- 恢复区域 -->
      <el-col :span="12">
        <el-card class="action-card restore-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span class="title">数据灾备恢复</span>
              <el-tag size="small" type="danger" effect="plain">高危操作</el-tag>
            </div>
          </template>

          <div class="backup-hero restore-hero">
            <div class="hero-icon">
              <el-icon :size="32" color="#F56C6C"><RefreshRight /></el-icon>
            </div>
            <div class="hero-text">
              <h3>需要回滚数据吗？</h3>
              <p>上传 ZIP 备份包，系统将自动清空并重建业务数据。<strong>此操作不可逆，请谨慎。</strong></p>
            </div>
          </div>

          <div class="action-footer compact-footer" style="margin-top: 54px;">
            <div class="last-time warning-text">
              <el-icon><Warning /></el-icon>
              恢复将覆盖当前记录
            </div>
            <el-upload
              action="#"
              :auto-upload="false"
              :show-file-list="false"
              accept=".zip"
              @change="handleRestore"
            >
              <template #trigger>
                <el-button 
                  type="danger" 
                  class="backup-btn"
                  size="small"
                  :loading="restoreLoading"
                >
                  <el-icon v-if="!restoreLoading" class="el-icon--left"><UploadIcon /></el-icon>
                  {{ restoreLoading ? '正在还原...' : '上传并恢复' }}
                </el-button>
              </template>
            </el-upload>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20">
      <!-- 说明区域 -->
      <el-col :span="24">
        <el-card class="info-card compact-info" shadow="never">
          <template #header>
            <div class="card-header">
              <span class="title">备份须知</span>
            </div>
          </template>
          
          <div class="info-flex-container">
            <div class="info-item-group">
              <div class="info-sub-title">备份包含内容</div>
              <ul class="info-list">
                <li><el-icon color="#67C23A"><CircleCheck /></el-icon> 用户账号及 RBAC 权限体系</li>
                <li><el-icon color="#67C23A"><CircleCheck /></el-icon> 教学资源元数据及审核记录</li>
                <li><el-icon color="#67C23A"><CircleCheck /></el-icon> 课程、章节、知识点层级结构</li>
                <li><el-icon color="#67C23A"><CircleCheck /></el-icon> 系统运行日志及配置参数</li>
              </ul>
            </div>

            <div class="info-item-group">
              <div class="info-sub-title">不包含内容</div>
              <div class="warning-text-box">
                <el-icon :size="16" color="#f56c6c"><Warning /></el-icon>
                <span>磁盘上的 PDF、DOCX、视频等<strong>原始资源文件</strong>不包含在此压缩包内。建议定期备份服务器 <code>/storage</code> 目录。</span>
              </div>
            </div>

            <div class="info-item-group">
              <div class="info-sub-title">运维建议</div>
              <div class="maintenance-box">
                <p>为确保教学数据安全，建议每周一进行全量备份。导出的文件应存放在与服务器物理隔离的设备中。</p>
              </div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<style scoped>
.backup-view {
  padding: 4px;
  min-height: calc(100vh - 120px);
  display: flex;
  flex-direction: column;
}

.status-row {
  margin-bottom: 20px;
}

.main-action-row {
  flex: 1;
  display: flex;
  margin-bottom: 20px;
}

.main-action-row .el-col {
  display: flex;
}

.action-card {
  width: 100%;
  display: flex;
  flex-direction: column;
  min-height: 360px;
}

:deep(.action-card .el-card__body) {
  flex: 1;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.mb-20 {
  margin-bottom: 20px;
}

.status-mini-card {
  border: none;
  background: #fff;
}

.mini-content {
  display: flex;
  align-items: center;
  gap: 15px;
}

.mini-content .label {
  font-size: 13px;
  color: #909399;
  margin-bottom: 4px;
}

.mini-content .status {
  font-size: 15px;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 6px;
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.card-header .title {
  font-weight: 700;
  font-size: 16px;
}

.backup-hero {
  display: flex;
  align-items: center;
  gap: 15px;
  padding: 20px 24px;
  background: linear-gradient(to right, #f0f7ff, #ffffff);
  border-radius: 8px;
  margin-bottom: 20px;
}

.hero-text h3 {
  margin: 0 0 6px 0;
  color: #303133;
  font-size: 15px;
}

.hero-text p {
  margin: 0;
  color: #606266;
  font-size: 13px;
  line-height: 1.5;
}

.restore-hero {
  background: linear-gradient(to right, #fff5f5, #ffffff);
}

.warning-text {
  color: #F56C6C !important;
  font-weight: 600;
}

.backup-steps {
  display: flex;
  justify-content: space-between;
  gap: 15px;
  margin-bottom: 24px;
}

.small-steps .step-item {
  padding: 12px;
  background: #f8f9fa;
  border-radius: 6px;
  flex: 1;
  display: flex;
  align-items: center;
  gap: 10px;
}

.small-steps .step-num {
  width: 22px;
  height: 22px;
  background: #409EFF;
  color: #fff;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 700;
}

.small-steps .step-content {
  font-size: 13px;
  color: #606266;
}

.action-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-top: 15px;
  border-top: 1px dashed #ebeef5;
}

.compact-footer {
  padding-top: 8px;
}

.last-time {
  font-size: 12px;
  color: #909399;
  display: flex;
  align-items: center;
  gap: 6px;
}

.last-time span {
  color: #303133;
  font-weight: 600;
}

.backup-btn {
  padding: 8px 16px;
  font-weight: 600;
}

.compact-info .info-flex-container {
  display: flex;
  gap: 30px;
  padding: 20px 0;
}

.compact-info .info-item-group {
  flex: 1;
}

.compact-info .info-sub-title {
  font-size: 14px;
  font-weight: 700;
  color: #303133;
  margin-bottom: 10px;
}

.compact-info .info-list {
  list-style: none;
  padding: 0;
  margin: 0;
}

.compact-info .info-list li {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: #606266;
  margin-bottom: 6px;
}

.compact-info .warning-text-box {
  background: #fff5f5;
  padding: 10px;
  border-radius: 6px;
  display: flex;
  gap: 10px;
  font-size: 12px;
  color: #f56c6c;
  line-height: 1.5;
}

.compact-info .maintenance-box {
  background: #f0f9eb;
  padding: 10px;
  border-radius: 6px;
  font-size: 12px;
  color: #67c23a;
  line-height: 1.5;
}
</style>
