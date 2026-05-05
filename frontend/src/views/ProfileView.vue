<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { 
  User, 
  Phone, 
  Lock, 
  Bell, 
  Edit, 
  Check, 
  Clock,
  CircleCheck,
  InfoFilled,
  Delete,
  Plus,
  Camera
} from '@element-plus/icons-vue'
import api from '../api/client'

const router = useRouter()

const loading = ref(false)
const user = ref(null)

const editOpen = ref(false)
const editForm = ref({ phone: '', password: '' })

const notifications = ref([])
const loadingNotifications = ref(false)
const notifPagination = ref({ page: 1, pageSize: 5, total: 0 })

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

const idLabel = computed(() => {
  if (roles.value.includes('student')) return '学号'
  return '工号'
})

const displayId = computed(() => {
  if (!user.value) return '-'
  return user.value.student_id || user.value.teacher_id || user.value.dean_id || user.value.id
})

const displayName = computed(() => {
  return user.value?.name || user.value?.username || '未登录'
})

const avatarUrl = computed(() => {
  if (!user.value?.avatar_url) return ''
  const baseUrl = api.defaults.baseURL || ''
  return baseUrl.endsWith('/') ? baseUrl.slice(0, -1) + user.value.avatar_url : baseUrl + user.value.avatar_url
})

async function handleAvatarUpload(options) {
  const { file } = options
  const formData = new FormData()
  formData.append('avatar', file)
  
  try {
    const resp = await api.post('/api/me/avatar', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
    if (user.value) {
      user.value.avatar_url = resp.data.avatar_url
      localStorage.setItem('user', JSON.stringify(user.value))
      window.dispatchEvent(new Event('user-updated'))
    }
    ElMessage.success('头像更新成功')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '头像更新失败')
  }
}

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
  editForm.value = { 
    name: user.value?.name || user.value?.username || '',
    phone: user.value?.phone || '', 
    password: '' 
  }
  editOpen.value = true
}

async function submitEdit() {
  const payload = {}
  const name = (editForm.value.name || '').trim()
  if (name) payload.name = name
  const phone = (editForm.value.phone || '').trim()
  if (phone) payload.phone = phone
  const password = (editForm.value.password || '').trim()
  if (password) payload.password = password

  if (Object.keys(payload).length === 0) {
    editOpen.value = false
    return
  }

  try {
    const resp = await api.put('/api/me', payload)
    ElMessage.success('保存成功')
    editOpen.value = false
    user.value = resp.data.user
    localStorage.setItem('user', JSON.stringify(user.value))
    window.dispatchEvent(new Event('user-updated'))
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  }
}

async function fetchNotifications() {
  loadingNotifications.value = true
  try {
    const resp = await api.get('/api/notifications', {
      params: {
        page: notifPagination.value.page,
        page_size: notifPagination.value.pageSize,
      },
    })
    notifications.value = resp.data.items || []
    notifPagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
  } catch (e) {
    notifications.value = []
  } finally {
    loadingNotifications.value = false
  }
}

function handleNotifPageChange(page) {
  notifPagination.value.page = page
  fetchNotifications()
}

function handleNotifSizeChange(size) {
  notifPagination.value.pageSize = size
  notifPagination.value.page = 1
  fetchNotifications()
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

async function deleteNotification(id) {
  try {
    await api.delete(`/api/notifications/${id}`)
    notifications.value = notifications.value.filter(n => n.id !== id)
    if (notifications.value.length === 0 && notifPagination.value.page > 1) {
      notifPagination.value.page -= 1
    }
    ElMessage.success('通知已删除')
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

async function deleteAllNotifications() {
  try {
    await ElMessageBox.confirm(
      '确定要清空所有通知吗？此操作不可恢复。',
      '提示',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )
    
    await api.delete('/api/notifications/all')
    notifications.value = []
    notifPagination.value.page = 1
    notifPagination.value.total = 0
    ElMessage.success('通知已全部清空')
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error('操作失败')
    }
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
            <el-upload
              class="avatar-uploader"
              action="#"
              :show-file-list="false"
              :http-request="handleAvatarUpload"
              accept="image/*"
            >
              <div class="avatar-wrapper">
                <el-avatar :size="100" :src="avatarUrl" :icon="User" class="avatar" />
                <div class="avatar-hover">
                  <el-icon><Camera /></el-icon>
                  <span>更换头像</span>
                </div>
              </div>
            </el-upload>
            <h2 class="username">{{ displayName }}</h2>
            <div class="role-tags">
              <el-tag v-for="r in (user?.roles || [])" :key="r" size="small" effect="dark" round>
                {{ r }}
              </el-tag>
            </div>
          </div>
          
          <div class="profile-info">
            <div class="info-item">
              <el-icon><InfoFilled /></el-icon>
              <span class="label">{{ idLabel }}</span>
              <span class="value">{{ displayId }}</span>
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
              <div class="header-actions">
                <el-button v-if="notifications.length > 0" size="small" :icon="CircleCheck" @click="markAllAsRead">
                  全部标记已读
                </el-button>
                <el-button v-if="notifications.length > 0" size="small" type="danger" plain :icon="Delete" @click="deleteAllNotifications">
                  清空通知
                </el-button>
              </div>
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
                      <div class="notif-title-group">
                        <span class="notif-title">{{ n.title }}</span>
                        <el-tag v-if="!n.is_read" size="small" type="danger" effect="dark" dot>新</el-tag>
                      </div>
                      <el-button 
                        size="small" 
                        type="danger" 
                        link 
                        :icon="Delete" 
                        class="delete-notif-btn" 
                        @click.stop="deleteNotification(n.id)"
                      />
                    </div>
                    <p class="notif-text">{{ n.content }}</p>
                  </div>
                </el-card>
              </el-timeline-item>
            </el-timeline>
          </div>

          <div class="table-pagination notif-pagination">
            <el-pagination
              v-model:current-page="notifPagination.page"
              v-model:page-size="notifPagination.pageSize"
              :total="notifPagination.total"
              :page-sizes="[5, 10, 20, 50]"
              layout="共, sizes, prev, pager, next, jumper"
              background
              @current-change="handleNotifPageChange"
              @size-change="handleNotifSizeChange"
            />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- Edit Dialog -->
    <el-dialog v-model="editOpen" title="编辑个人信息" width="400px" border-radius="12px">
      <el-form :model="editForm" label-position="top">
        <el-form-item label="姓名">
          <el-input v-model="editForm.name" placeholder="请输入姓名" />
        </el-form-item>
        <el-form-item label="手机号">
          <el-input v-model="editForm.phone" placeholder="请输入手机号" />
        </el-form-item>
        <el-form-item label="新密码 (不修改请留空)">
          <el-input v-model="editForm.password" type="password" placeholder="请输入新密码" show-password />
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

.avatar-uploader {
  display: inline-block;
  cursor: pointer;
}

.avatar-wrapper {
  position: relative;
  width: 100px;
  height: 100px;
  margin: 0 auto 16px;
  border-radius: 50%;
  overflow: hidden;
}

.avatar {
  background: #f0f7ff;
  color: #409eff;
  border: 4px solid #fff;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  transition: all 0.3s;
}

.avatar-hover {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  color: #fff;
  opacity: 0;
  transition: opacity 0.3s;
  font-size: 12px;
  gap: 4px;
}

.avatar-wrapper:hover .avatar-hover {
  opacity: 1;
}

.avatar-wrapper:hover .avatar {
  transform: scale(1.1);
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

.notification-list {
  min-height: 560px;
}

.notification-list :deep(.el-timeline-item__content) {
  margin-left: 10px;
}

.notification-list :deep(.el-timeline-item__tail) {
  left: 6px;
}

.notification-list :deep(.el-timeline-item__node) {
  left: 1px;
}

.notification-list :deep(.el-timeline-item) {
  padding-bottom: 8px;
}

.notification-item {
  width: 100%;
  max-width: 100%;
  min-height: 86px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-actions {
  display: flex;
  gap: 12px;
}

.title-with-icon {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  font-size: 16px;
}

.notification-list {
  padding: 6px 0;
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
  margin-bottom: 6px;
}

.notif-title-group {
  display: flex;
  align-items: center;
  gap: 8px;
}

.delete-notif-btn {
  opacity: 0;
  transition: opacity 0.3s;
}

.notification-item:hover .delete-notif-btn {
  opacity: 1;
}

.notif-title {
  font-weight: 600;
  color: #303133;
}

.notif-text {
  margin: 0;
  font-size: 13px;
  color: #606266;
  line-height: 1.45;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}
</style>
