<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { 
  Download, 
  Warning, 
  Management, 
  CircleCheck, 
  Clock, 
  Cpu, 
  Connection,
  FolderChecked,
  InfoFilled
} from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
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

    <el-row :gutter="20">
      <!-- 导出区域 -->
      <el-col :span="24" class="mb-20">
        <el-card class="action-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span class="title">全量数据导出</span>
              <el-tag size="small" type="success" effect="plain">推荐操作</el-tag>
            </div>
          </template>

          <div class="backup-hero">
            <div class="hero-icon">
              <el-icon :size="40" color="#409EFF"><Management /></el-icon>
            </div>
            <div class="hero-text">
              <h3>准备好保护您的数据了吗？</h3>
              <p>系统将自动梳理 MySQL 数据库中的所有业务实体，包括用户权限、资源元数据及关联关系，打包成 ZIP 压缩包供您本地存档。</p>
            </div>
          </div>

          <div class="backup-steps">
            <div class="step-item">
              <div class="step-num">1</div>
              <div class="step-content">扫描 12+ 张业务数据表</div>
            </div>
            <div class="step-item">
              <div class="step-num">2</div>
              <div class="step-content">序列化为结构化 JSON 格式</div>
            </div>
            <div class="step-item">
              <div class="step-num">3</div>
              <div class="step-content">执行 ZIP 标准无损压缩</div>
            </div>
          </div>

          <div class="action-footer">
            <div class="last-time">
              <el-icon><Clock /></el-icon>
              上次备份时间：<span>{{ lastBackupTime }}</span>
            </div>
            <el-button 
              type="primary" 
              class="backup-btn"
              :loading="loading"
              @click="handleBackup"
            >
              <el-icon v-if="!loading" class="el-icon--left"><Download /></el-icon>
              {{ loading ? '正在打包数据...' : '立即开始导出' }}
            </el-button>
          </div>
        </el-card>
      </el-col>

      <!-- 说明区域 -->
      <el-col :span="24">
        <el-card class="info-card" shadow="never">
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
}

.mb-20 {
  margin-bottom: 20px;
}

.status-row {
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
  gap: 20px;
  padding: 16px 20px;
  background: linear-gradient(to right, #f0f7ff, #ffffff);
  border-radius: 12px;
  margin-bottom: 20px;
}

.hero-text h3 {
  margin: 0 0 4px 0;
  color: #303133;
  font-size: 16px;
}

.hero-text p {
  margin: 0;
  color: #606266;
  line-height: 1.6;
  font-size: 14px;
}

.backup-steps {
  display: flex;
  justify-content: space-around;
  margin-bottom: 40px;
  padding: 0 40px;
}

.step-item {
  text-align: center;
  flex: 1;
  position: relative;
}

.step-item:not(:last-child)::after {
  content: '';
  position: absolute;
  top: 15px;
  right: -50%;
  width: 100%;
  height: 2px;
  background: #ebeef5;
  z-index: 0;
}

.step-num {
  width: 32px;
  height: 32px;
  background: #409EFF;
  color: #fff;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto 10px;
  font-weight: bold;
  position: relative;
  z-index: 1;
  box-shadow: 0 4px 10px rgba(64, 158, 255, 0.3);
}

.step-content {
  font-size: 13px;
  color: #606266;
}

.action-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 24px;
  background-color: #f8f9fa;
  border-radius: 8px;
}

.last-time {
  font-size: 14px;
  color: #909399;
  display: flex;
  align-items: center;
  gap: 6px;
}

.last-time span {
  color: #303133;
  font-weight: 500;
}

.backup-btn {
  padding-left: 30px;
  padding-right: 30px;
  height: 40px;
  font-size: 14px;
  font-weight: 600;
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(64, 158, 255, 0.2);
}

/* 说明区域 Flex 布局 */
.info-flex-container {
  display: flex;
  gap: 30px;
  padding: 10px 0;
}

.info-item-group {
  flex: 1;
}

.info-sub-title {
  font-weight: 600;
  font-size: 14px;
  color: #303133;
  margin-bottom: 15px;
  padding-left: 10px;
  border-left: 4px solid #409EFF;
}

.info-list {
  list-style: none;
  padding: 0;
  margin: 0;
}

.info-list li {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
  font-size: 14px;
  color: #606266;
}

.warning-text-box {
  background: #fff6f7;
  padding: 12px 15px;
  border-radius: 6px;
  color: #f56c6c;
  font-size: 13px;
  line-height: 1.5;
  display: flex;
  align-items: flex-start;
  gap: 8px;
}

.warning-text-box span {
  flex: 1;
  word-break: normal;
}

.maintenance-box {
  padding: 15px;
  background-color: #fdf6ec;
  border-radius: 8px;
  color: #e6a23c;
  font-size: 13px;
  line-height: 1.6;
}

.maintenance-box p {
  margin: 0;
}
</style>
