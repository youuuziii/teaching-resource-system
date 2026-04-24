<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { 
  Download, 
  Star, 
  StarFilled, 
  ArrowLeft,
  Calendar,
  User,
  Reading,
  PriceTag,
  Document,
  Check,
  Close,
  Timer
} from '@element-plus/icons-vue'
import api from '../api/client'

const route = useRoute()
const router = useRouter()
const id = computed(() => Number(route.params.id))

const loading = ref(false)
const resource = ref(null)
const createdByUser = ref(null)
const auditedByUser = ref(null)

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)
const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const canFavorite = computed(() => isAuthed.value && roles.value.includes('student'))

async function fetchDetail() {
  loading.value = true
  try {
    const resp = await api.get(`/api/resources/${id.value}`)
    resource.value = resp.data.resource
    createdByUser.value = resp.data.created_by_user
    auditedByUser.value = resp.data.audited_by_user
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function goBack() {
  router.back()
}

function download() {
  if (!resource.value) return
  const token = localStorage.getItem('token')
  const baseUrl = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'
  const url = `${baseUrl}/api/resources/${resource.value.id}/download?token=${token}`
  window.open(url, '_blank')
}

async function toggleFavorite() {
  if (!resource.value) return
  const action = resource.value.is_favorited ? 'unfavorite' : 'favorite'
  try {
    await api.post(`/api/resources/${resource.value.id}/favorite`, { action })
    resource.value.is_favorited = !resource.value.is_favorited
    ElMessage.success(action === 'favorite' ? '已收藏' : '已取消收藏')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

const statusType = computed(() => {
  if (!resource.value) return 'info'
  switch (resource.value.status) {
    case 'approved': return 'success'
    case 'pending': return 'warning'
    case 'rejected': return 'danger'
    default: return 'info'
  }
})

const statusLabel = computed(() => {
  if (!resource.value) return ''
  switch (resource.value.status) {
    case 'approved': return '已审核通过'
    case 'pending': return '审核中'
    case 'rejected': return '已拒绝'
    default: return resource.value.status
  }
})

onMounted(fetchDetail)
</script>

<template>
  <div class="detail-container">
    <!-- Header Actions -->
    <div class="detail-actions">
      <el-button :icon="ArrowLeft" @click="goBack">返回列表</el-button>
      <div class="right-actions">
        <el-button 
          v-if="resource && canFavorite" 
          :type="resource.is_favorited ? 'warning' : 'default'"
          :icon="resource.is_favorited ? StarFilled : Star"
          @click="toggleFavorite"
        >
          {{ resource.is_favorited ? '取消收藏' : '加入收藏' }}
        </el-button>
        <el-button 
          v-if="resource" 
          type="primary" 
          :icon="Download" 
          :disabled="!isAuthed" 
          @click="download"
        >
          下载资源
        </el-button>
      </div>
    </div>

    <el-skeleton :loading="loading" animated>
      <template #default>
        <div v-if="resource" class="detail-content">
          <el-row :gutter="24">
            <!-- Left: Main Info -->
            <el-col :xs="24" :md="16">
              <el-card shadow="never" class="info-card main-info">
                <div class="title-row">
                  <el-tag :type="statusType" effect="dark" class="status-tag">
                    {{ statusLabel }}
                  </el-tag>
                  <h1 class="title">{{ resource.title }}</h1>
                </div>
                
                <div class="description-section">
                  <h3 class="section-title">资源描述</h3>
                  <p class="description-text">{{ resource.description || '暂无详细描述。' }}</p>
                </div>

                <div class="tags-section">
                  <h3 class="section-title">资源标签</h3>
                  <div class="tags-list">
                    <el-tag v-for="t in resource.tags || []" :key="t" size="default" effect="plain" class="tag-item">
                      {{ t }}
                    </el-tag>
                    <span v-if="!resource.tags?.length" class="empty-text">无标签</span>
                  </div>
                </div>
              </el-card>

              <!-- Audit Info (if rejected or special note) -->
              <el-card v-if="resource.audit_comment" shadow="never" class="info-card audit-card">
                <template #header>
                  <div class="card-header">
                    <el-icon><Check /></el-icon>
                    <span>审核反馈</span>
                  </div>
                </template>
                <p class="audit-comment">{{ resource.audit_comment }}</p>
              </el-card>
            </el-col>

            <!-- Right: Sidebar Meta -->
            <el-col :xs="24" :md="8">
              <el-card shadow="never" class="info-card meta-sidebar">
                <h3 class="sidebar-title">基本信息</h3>
                <div class="meta-list">
                  <div class="meta-item">
                    <el-icon><Reading /></el-icon>
                    <div class="meta-label">所属课程</div>
                    <div class="meta-value">{{ resource.course || '通用' }}</div>
                  </div>
                  <div class="meta-item">
                    <el-icon><Timer /></el-icon>
                    <div class="meta-label">关联知识点</div>
                    <div class="meta-value">{{ resource.knowledge_point || '-' }}</div>
                  </div>
                  <div class="meta-item">
                    <el-icon><User /></el-icon>
                    <div class="meta-label">上传教师</div>
                    <div class="meta-value">
                      <el-tag v-for="t in resource.teachers || []" :key="t.id" size="small" effect="plain">
                        {{ t.name }}
                      </el-tag>
                      <span v-if="!resource.teachers?.length">-</span>
                    </div>
                  </div>
                  <div class="meta-item">
                    <el-icon><Document /></el-icon>
                    <div class="meta-label">文件信息</div>
                    <div class="meta-value file-info">
                      <span>{{ resource.file_name }}</span>
                      <span class="file-size">({{ resource.file_size || '未知大小' }})</span>
                    </div>
                  </div>
                  <div class="meta-item">
                    <el-icon><Calendar /></el-icon>
                    <div class="meta-label">上传时间</div>
                    <div class="meta-value">{{ new Date(resource.created_at).toLocaleString() }}</div>
                  </div>
                </div>

                <el-divider />
                
                <h3 class="sidebar-title">审核流程</h3>
                <div class="meta-list">
                  <div class="meta-item">
                    <el-icon><User /></el-icon>
                    <div class="meta-label">审核人</div>
                    <div class="meta-value">{{ auditedByUser?.username || '-' }}</div>
                  </div>
                  <div class="meta-item">
                    <el-icon><Calendar /></el-icon>
                    <div class="meta-label">审核时间</div>
                    <div class="meta-value">{{ resource.audited_at ? new Date(resource.audited_at).toLocaleString() : '等待中' }}</div>
                  </div>
                </div>
              </el-card>
            </el-col>
          </el-row>
        </div>
      </template>
    </el-skeleton>
  </div>
</template>

<style scoped>
.detail-container {
  max-width: 1200px;
  margin: 0 auto;
}

.detail-actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}

.right-actions {
  display: flex;
  gap: 12px;
}

.info-card {
  border-radius: 12px;
  margin-bottom: 24px;
}

.main-info {
  padding: 10px;
}

.title-row {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  margin-bottom: 30px;
}

.status-tag {
  margin-top: 4px;
}

.title {
  margin: 0;
  font-size: 24px;
  font-weight: 700;
  color: #303133;
  line-height: 1.4;
}

.section-title {
  font-size: 16px;
  font-weight: 600;
  color: #303133;
  margin: 0 0 16px;
  padding-left: 12px;
  border-left: 4px solid #409eff;
}

.description-section {
  margin-bottom: 40px;
}

.description-text {
  font-size: 15px;
  color: #606266;
  line-height: 1.8;
  white-space: pre-wrap;
}

.tags-list {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.tag-item {
  border-radius: 6px;
}

.audit-card {
  border-left: 4px solid #f56c6c;
}

.audit-comment {
  font-size: 14px;
  color: #f56c6c;
  line-height: 1.6;
}

.meta-sidebar {
  padding: 5px;
}

.sidebar-title {
  font-size: 15px;
  font-weight: 600;
  color: #303133;
  margin: 0 0 20px;
}

.meta-list {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.meta-item {
  display: flex;
  gap: 12px;
  align-items: flex-start;
}

.meta-item .el-icon {
  margin-top: 2px;
  color: #909399;
}

.meta-label {
  font-size: 13px;
  color: #909399;
  width: 70px;
  flex-shrink: 0;
}

.meta-value {
  font-size: 14px;
  color: #303133;
  font-weight: 500;
  word-break: break-all;
}

.file-info {
  display: flex;
  flex-direction: column;
}

.file-size {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}

.empty-text {
  font-size: 14px;
  color: #c0c4cc;
}
</style>
