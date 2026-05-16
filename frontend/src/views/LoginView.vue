<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { User, Lock, UserFilled, Clock, Close } from '@element-plus/icons-vue'
import api from '../api/client'
import logo from '../assets/logo.png'

const router = useRouter()
const loading = ref(false)
const rememberMe = ref(false)
const savedAccounts = ref([])
const showSavedDropdown = ref(false)
const usernameInputRef = ref(null)
const usernameFieldRef = ref(null)
const form = reactive({
  username: '',
  password: '',
  role: '',
})

const roleOptions = [
  { label: '学生', value: 'student' },
  { label: '教师', value: 'teacher' },
  { label: '教务管理员', value: 'dean' },
  { label: '系统管理员', value: 'admin' },
]

const selectedRoleLabel = computed(() => roleOptions.find((r) => r.value === form.role)?.label || '')

function loadSavedAccounts() {
  try {
    const raw = JSON.parse(localStorage.getItem('remembered_accounts') || '[]')
    savedAccounts.value = Array.isArray(raw) ? raw : []
  } catch {
    savedAccounts.value = []
  }
}

function persistSavedAccounts(list) {
  savedAccounts.value = list
  localStorage.setItem('remembered_accounts', JSON.stringify(list))
}

function openSavedDropdown() {
  if (savedAccounts.value.length > 0) {
    showSavedDropdown.value = true
  }
}

function closeSavedDropdown() {
  showSavedDropdown.value = false
}

function handleDocumentClick(event) {
  const target = event?.target
  const inputEl = usernameFieldRef.value?.$el || usernameInputRef.value?.$el
  const dropdownEl = document.querySelector('.saved-dropdown')
  if (!showSavedDropdown.value) return
  if (inputEl && inputEl.contains(target)) return
  if (dropdownEl && dropdownEl.contains(target)) return
  closeSavedDropdown()
}

function selectSavedAccount(item) {
  if (!item) return
  form.username = item.username || ''
  form.password = item.password || ''
  form.role = item.role || ''
  rememberMe.value = true
  closeSavedDropdown()
}

function removeSavedAccount(item) {
  if (!item) return
  const nextAccounts = savedAccounts.value.filter((saved) => !(saved.username === item.username && saved.role === item.role))
  persistSavedAccounts(nextAccounts)
  if (!nextAccounts.length) {
    localStorage.removeItem('remembered_login')
  }
  if (form.username === item.username && form.role === item.role) {
    form.username = ''
    form.password = ''
    form.role = ''
  }
  ElMessage.success('已移除该记住的账号')
}

onMounted(() => {
  loadSavedAccounts()
  document.addEventListener('click', handleDocumentClick)
  try {
    const saved = JSON.parse(localStorage.getItem('remembered_login') || 'null')
    if (saved?.username) form.username = saved.username
    if (saved?.password) form.password = saved.password
    if (saved?.role) form.role = saved.role
    rememberMe.value = !!saved
  } catch {
    rememberMe.value = false
  }
})

onBeforeUnmount(() => {
  document.removeEventListener('click', handleDocumentClick)
})

async function submit() {
  if (!form.username || !form.password) {
    ElMessage.warning('请输入用户名/学号/职工号和密码')
    return
  }
  if (!form.role) {
    ElMessage.warning('请选择登录角色')
    return
  }
  loading.value = true
  try {
    const resp = await api.post('/api/auth/login', {
      username: form.username,
      password: form.password,
    })
    const userRoles = Array.isArray(resp.data?.user?.roles) ? resp.data.user.roles : []
    if (!userRoles.includes(form.role)) {
      ElMessage.error(`该账号不具备「${selectedRoleLabel.value}」角色，请重新选择或联系管理员`)
      return
    }
    localStorage.setItem('token', resp.data.token)
    localStorage.setItem('user', JSON.stringify({ ...resp.data.user, current_role: form.role }))
    localStorage.setItem('current_role', form.role)
    if (rememberMe.value) {
      const nextAccount = { username: form.username, password: form.password, role: form.role }
      const nextAccounts = [
        nextAccount,
        ...savedAccounts.value.filter((item) => !(item.username === nextAccount.username && item.role === nextAccount.role)),
      ].slice(0, 10)
      persistSavedAccounts(nextAccounts)
      localStorage.setItem('remembered_login', JSON.stringify(nextAccount))
    } else {
      localStorage.removeItem('remembered_login')
    }
    window.dispatchEvent(new Event('user-updated'))
    ElMessage.success(`欢迎回来，当前登录角色：${selectedRoleLabel.value}`)
    router.replace('/')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '登录失败，请检查账号密码')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-container">
    <div class="login-box">
      <div class="login-header">
        <div class="logo-wrapper">
          <img :src="logo" alt="Logo" class="logo-img" />
        </div>
        <h2>教学资源管理系统</h2>
        <p>基于知识图谱的智能化教学平台</p>
      </div>
      
      <el-card class="login-card" shadow="always">
        <el-form :model="form" size="large" @submit.prevent="submit">
          <el-form-item>
            <el-popover
              :visible="showSavedDropdown && savedAccounts.length > 0"
              placement="bottom-start"
              :teleported="false"
              :width="360"
              trigger="manual"
            >
              <template #reference>
                <el-input 
                  ref="usernameInputRef"
                  v-model="form.username" 
                  placeholder="用户名 / 学号 / 职工号" 
                  :prefix-icon="User"
                  autocomplete="username"
                  @focus="openSavedDropdown"
                  @click="openSavedDropdown"
                />
              </template>
              <div class="saved-dropdown">
                <div class="saved-dropdown__title">
                  <el-icon><Clock /></el-icon>
                  <span>已记住的账号</span>
                </div>
                <div class="saved-dropdown__list">
                  <div
                    v-for="item in savedAccounts"
                    :key="`${item.username}-${item.role}`"
                    class="saved-dropdown__item"
                    @mousedown.prevent="selectSavedAccount(item)"
                  >
                    <div class="saved-dropdown__main">
                      <span class="saved-dropdown__username">{{ item.username }}</span>
                      <span class="saved-dropdown__role">{{ roleOptions.find((r) => r.value === item.role)?.label || item.role }}</span>
                    </div>
                    <el-icon class="saved-dropdown__close" @mousedown.stop.prevent="removeSavedAccount(item)"><Close /></el-icon>
                  </div>
                </div>
              </div>
            </el-popover>
          </el-form-item>
          <el-form-item>
            <el-input 
              v-model="form.password" 
              type="password" 
              placeholder="密码" 
              :prefix-icon="Lock"
              autocomplete="current-password" 
              show-password 
              @keyup.enter="submit"
            />
          </el-form-item>
          <el-form-item>
            <el-select v-model="form.role" placeholder="请选择登录角色" style="width: 100%">
              <template #prefix><el-icon><UserFilled /></el-icon></template>
              <el-option v-for="role in roleOptions" :key="role.value" :label="role.label" :value="role.value" />
            </el-select>
          </el-form-item>
          <div class="form-options">
            <el-checkbox v-model="rememberMe">记住我</el-checkbox>
            <el-link type="primary" :underline="false">忘记密码？</el-link>
          </div>
          <el-button 
            type="primary" 
            class="login-button" 
            :loading="loading" 
            @click="submit"
          >
            立即登录
          </el-button>
        </el-form>
      </el-card>
      
      <div class="login-footer">
        <span>还没有账号？</span>
        <el-link type="primary" :underline="false">联系管理员创建</el-link>
      </div>
    </div>
  </div>
</template>

<style scoped>
.login-container {
  height: 100vh;
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1000;
}

.login-box {
  width: 400px;
  animation: fadeIn 0.8s ease-out;
}

.login-header {
  text-align: center;
  margin-bottom: 30px;
}

.logo-wrapper {
  display: inline-flex;
  justify-content: center;
  align-items: center;
  width: 72px;
  height: 72px;
  background: white;
  border-radius: 16px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
  margin-bottom: 16px;
  overflow: hidden;
}

.logo-img {
  width: 100%;
  height: 100%;
  object-fit: contain;
  padding: 8px;
}

.login-header h2 {
  margin: 0;
  font-size: 24px;
  color: #303133;
  font-weight: 600;
}

.login-header p {
  margin: 8px 0 0;
  font-size: 14px;
  color: #909399;
}

.login-card {
  border: none;
  border-radius: 12px;
  padding: 10px;
}

.saved-dropdown {
  padding: 8px 4px;
}

.saved-dropdown__title {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #606266;
  font-size: 13px;
  margin-bottom: 10px;
  padding: 0 4px;
}

.saved-dropdown__list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 220px;
  overflow: auto;
}

.saved-dropdown__item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  border: 1px solid #e4e7ed;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.saved-dropdown__item:hover {
  background: #f5f7ff;
  border-color: #cfd7ff;
}

.saved-dropdown__main {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.saved-dropdown__username {
  color: #303133;
  font-weight: 600;
}

.saved-dropdown__role {
  color: #909399;
  font-size: 12px;
}

.saved-dropdown__close {
  cursor: pointer;
  opacity: 0.7;
  flex-shrink: 0;
}

.saved-dropdown__close:hover {
  opacity: 1;
}

.form-options {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.login-button {
  width: 100%;
  height: 45px;
  font-size: 16px;
  border-radius: 8px;
  letter-spacing: 2px;
}

.login-footer {
  text-align: center;
  margin-top: 24px;
  font-size: 14px;
  color: #606266;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 适配移动端 */
@media (max-width: 480px) {
  .login-box {
    width: 90%;
  }
}
</style>
