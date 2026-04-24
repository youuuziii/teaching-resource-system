<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { 
  User, 
  Phone, 
  Lock, 
  Bell, 
  Edit, 
  Check, 
  Clock,
  CircleCheck,
  InfoFilled
} from '@element-plus/icons-vue'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const user = ref(null)

const editOpen = ref(false)
const editForm = ref({ phone: '', password: '' })

const notifications = ref([])
const loadingNotifications = ref(false)

const isAuthed = computed(() => (localStorage.getItem('token') || '').length > 0)
const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isStudent = computed(() => roles.value.includes('student'))

function openDetail(res) {
  router.push(`/resources/${res.id}`)
}

async function fetchMe() {
  loading.value = true
  try {
    const resp = await api.get('/api/me')
    user.value = resp.data.user
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function openEdit() {
  editForm.value = { phone: user.value?.phone || '', password: '' }
  editOpen.value = true
}

async function submitEdit() {
  const payload = {}
  const phone = (editForm.value.phone || '').trim()
  if (phone) payload.phone = phone
  const password = (editForm.value.password || '').trim()
  if (password) payload.password = password

  if (Object.keys(payload).length === 0) {
    editOpen.value = false
    return
  }

  try {
    await api.put('/api/me', payload)
    ElMessage.success('保存成功')
    editOpen.value = false
    await fetchMe()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function fetchNotifications() {
  loadingNotifications.value = true
  try {
    const resp = await api.get('/api/notifications')
    notifications.value = resp.data.items || []
  } catch (e) {
    notifications.value = []
  } finally {
    loadingNotifications.value = false
  }
}

async function markAsRead(notification) {
  if (notification.is_read) return
  try {
    await api.post(`/api/notifications/${notification.id}/read`)
    notification.is_read = true
  } catch (e) {
    ElMessage.error('标记已读失败')
  }
}

async function markAllAsRead() {
  try {
    await api.post('/api/notifications/read-all')
    notifications.value.forEach(n => n.is_read = true)
    ElMessage.success('全部标记为已读')
  } catch (e) {
    ElMessage.error('操作失败')
  }
}

onMounted(async () => {
  if (!isAuthed.value) return
  await fetchMe()
  await fetchNotifications()
})
</script>

<template>
  <div class="profile-container">
    <el-row :gutter="24">
      <!-- Left: User Profile -->
      <el-col :xs="24" :md="8">
        <el-card class="user-profile-card" shadow="never">
          <div class="profile-header">
            <el-avatar :size="80" :icon="User" class="avatar" />
            <h2 class="username">{{ user?.username || '未登录' }}</h2>
            <div class="role-tags">
              <el-tag v-for="r in (user?.roles || [])" :key="r" size="small" effect="dark" round>
                {{ r }}
              </el-tag>
            </div>
          </div>
          
          <div class="profile-info">
            <div class="info-item">
              <el-icon><InfoFilled /></el-icon>
              <span class="label">用户 ID</span>
              <span class="value">{{ user?.id || '-' }}</span>
            </div>
            <div class="info-item">
              <el-icon><Phone /></el-icon>
              <span class="label">手机号</span>
              <span class="value">{{ user?.phone || '未设置' }}</span>
            </div>
            <div class="info-item">
              <el-icon><Clock /></el-icon>
              <span class="label">注册时间</span>
              <span class="value">{{ user?.created_at ? new Date(user.created_at).toLocaleDateString() : '-' }}</span>
            </div>
          </div>

          <el-button class="edit-btn" :icon="Edit" @click="openEdit" block>
            编辑个人资料
          </el-button>
        </el-card>
      </el-col>

      <!-- Right: Notifications -->
      <el-col :xs="24" :md="16">
        <el-card class="notification-card" shadow="never">
          <template #header>
            <div class="card-header">
              <div class="title-with-icon">
                <el-icon><Bell /></el-icon>
                <span>消息通知</span>
              </div>
              <el-button v-if="notifications.length > 0" size="small" :icon="CircleCheck" @click="markAllAsRead">
                全部标记已读
              </el-button>
            </div>
          </template>

          <div v-loading="loadingNotifications" class="notification-list">
            <el-empty v-if="notifications.length === 0" description="暂无通知消息" />
            <el-timeline v-else>
              <el-timeline-item
                v-for="n in notifications"
                :key="n.id"
                :timestamp="new Date(n.created_at).toLocaleString()"
                :type="n.is_read ? 'info' : 'primary'"
                :hollow="n.is_read"
                placement="top"
              >
                <el-card shadow="hover" class="notification-item" :class="{ 'unread': !n.is_read }" @click="markAsRead(n)">
                  <div class="notification-content">
                    <div class="notif-header">
                      <span class="notif-title">{{ n.title }}</span>
                      <el-tag v-if="!n.is_read" size="small" type="danger" effect="dark" dot>新</el-tag>
                    </div>
                    <p class="notif-text">{{ n.content }}</p>
                  </div>
                </el-card>
              </el-timeline-item>
            </el-timeline>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- Edit Dialog -->
    <el-dialog v-model="editOpen" title="编辑个人信息" width="400px" border-radius="12px">
      <el-form :model="editForm" label-position="top">
        <el-form-item label="手机号">
          <el-input v-model="editForm.phone" placeholder="请输入手机号" :prefix-icon="Phone" />
        </el-form-item>
        <el-form-item label="修改密码">
          <el-input v-model="editForm.password" placeholder="留空则不修改" :prefix-icon="Lock" show-password />
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="editOpen = false">取消</el-button>
          <el-button type="primary" @click="submitEdit">保存更改</el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.profile-container {
  padding: 0;
}

.user-profile-card {
  border-radius: 12px;
  text-align: center;
  padding: 20px 0;
}

.profile-header {
  margin-bottom: 30px;
}

.avatar {
  background: #f0f7ff;
  color: #409eff;
  margin-bottom: 16px;
  border: 4px solid #fff;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

.username {
  margin: 0 0 12px;
  font-size: 20px;
  color: #303133;
}

.role-tags {
  display: flex;
  justify-content: center;
  gap: 8px;
  flex-wrap: wrap;
}

.profile-info {
  text-align: left;
  padding: 0 20px;
  margin-bottom: 30px;
}

.info-item {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  font-size: 14px;
}

.info-item .el-icon {
  color: #909399;
}

.info-item .label {
  color: #909399;
  width: 70px;
}

.info-item .value {
  color: #303133;
  font-weight: 500;
}

.edit-btn {
  width: calc(100% - 40px);
  margin: 0 20px;
  height: 40px;
  border-radius: 8px;
}

.notification-card {
  border-radius: 12px;
  min-height: 500px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-with-icon {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  font-size: 16px;
}

.notification-list {
  padding: 10px 0;
}

.notification-item {
  border-radius: 8px;
  cursor: pointer;
  border: 1px solid #f0f2f5;
  transition: all 0.3s;
}

.notification-item:hover {
  border-color: #409eff;
  background: #fdfdfd;
}

.notification-item.unread {
  border-left: 4px solid #409eff;
  background: #f0f7ff;
}

.notif-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.notif-title {
  font-weight: 600;
  color: #303133;
}

.notif-text {
  margin: 0;
  font-size: 14px;
  color: #606266;
  line-height: 1.6;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}
</style>
