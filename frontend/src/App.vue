<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { 
  Collection, 
  Share, 
  User, 
  UserFilled, 
  Setting, 
  Bell, 
  Checked, 
  Management, 
  Memo,
  SwitchButton,
  Notebook
} from '@element-plus/icons-vue'
import logo from './assets/logo.png'

const route = useRoute()
const router = useRouter()

const user = computed(() => {
  try {
    return JSON.parse(localStorage.getItem('user') || '{}')
  } catch {
    return {}
  }
})

const token = computed(() => localStorage.getItem('token') || '')
const isAuthed = computed(() => token.value.length > 0)
const roles = computed(() => user.value.roles || [])

const isSystemAdmin = computed(() => roles.value.includes('admin'))
const isDean = computed(() => roles.value.includes('dean'))
const isStudent = computed(() => roles.value.includes('student'))
const isTeacherOnly = computed(() => roles.value.includes('teacher'))

function go(path) {
  router.push(path)
}

function logout() {
  localStorage.removeItem('token')
  localStorage.removeItem('user')
  router.push('/login')
}
</script>

<template>
  <el-container class="app-container">
    <el-header class="app-header">
      <div class="header-content">
        <div class="logo-section" @click="go('/')">
          <img :src="logo" alt="Logo" class="nav-logo" />
          <span class="title">教学资源管理系统</span>
        </div>
        
        <el-menu 
          mode="horizontal" 
          :default-active="route.path" 
          class="nav-menu"
          :ellipsis="false"
          @select="go"
        >
          <el-menu-item index="/">
            <el-icon><Collection /></el-icon>
            <span>资源中心</span>
          </el-menu-item>
          <el-menu-item index="/graph">
            <el-icon><Share /></el-icon>
            <span>知识图谱</span>
          </el-menu-item>
          <el-menu-item v-if="isStudent" index="/learning">
            <el-icon><Notebook /></el-icon>
            <span>学习推荐</span>
          </el-menu-item>
          
          <!-- Teacher Management -->
          <el-menu-item v-if="isTeacherOnly" index="/teacher/courses">
            <el-icon><Memo /></el-icon>
            <span>课程管理</span>
          </el-menu-item>
          
          <!-- Dean/Admin Audit & Assignment -->
          <el-menu-item v-if="isDean" index="/admin/audit">
            <el-icon><Checked /></el-icon>
            <span>资源审核</span>
          </el-menu-item>
          <el-menu-item v-if="isDean || isSystemAdmin" index="/admin/courses">
            <el-icon><Management /></el-icon>
            <span>课程分配</span>
          </el-menu-item>
          
          <!-- System Admin -->
          <el-menu-item v-if="isSystemAdmin" index="/admin/logs">
            <el-icon><Setting /></el-icon>
            <span>系统日志</span>
          </el-menu-item>
          <el-menu-item v-if="isSystemAdmin" index="/admin/users">
            <el-icon><User /></el-icon>
            <span>账号权限</span>
          </el-menu-item>
          <el-menu-item v-else-if="isDean" index="/admin/users">
            <el-icon><User /></el-icon>
            <span>账号管理</span>
          </el-menu-item>
        </el-menu>

        <div class="user-section">
          <template v-if="isAuthed">
            <el-dropdown trigger="click">
              <div class="user-info">
                <el-avatar :size="32" :icon="UserFilled" />
                <span class="username">{{ user.username }}</span>
              </div>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item @click="go('/profile')">
                    <el-icon><User /></el-icon>个人中心
                  </el-dropdown-item>
                  <el-dropdown-item divided @click="logout">
                    <el-icon><SwitchButton /></el-icon>退出登录
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </template>
          <el-button v-else type="primary" size="small" @click="go('/login')">登录</el-button>
        </div>
      </div>
    </el-header>
    
    <el-main class="app-main">
      <div class="main-content">
        <router-view />
      </div>
    </el-main>
  </el-container>
</template>

<style>
:root {
  --primary-color: #409eff;
  --header-height: 64px;
  --bg-color: #f5f7fa;
}

body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  background-color: var(--bg-color);
}

.app-container {
  min-height: 100vh;
}

.app-header {
  height: var(--header-height);
  background: #fff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.06);
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: 1000;
  padding: 0 20px;
}

.header-content {
  max-width: 1400px;
  margin: 0 auto;
  height: 100%;
  display: flex;
  align-items: center;
}

.logo-section {
  display: flex;
  align-items: center;
  cursor: pointer;
  padding: 0 10px;
}

.nav-logo {
  width: 32px;
  height: 32px;
  object-fit: contain;
  margin-right: 10px;
}

.logo-section .title {
  font-size: 18px;
  font-weight: 700;
  color: #303133;
  white-space: nowrap;
}

.nav-menu {
  flex: 1;
  border-bottom: none !important;
  height: 100%;
}

.nav-menu .el-menu-item {
  height: var(--header-height);
  line-height: var(--header-height);
  font-size: 15px;
}

.user-section {
  margin-left: 20px;
}

.user-info {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  padding: 4px 8px;
  border-radius: 4px;
  transition: background 0.3s;
}

.user-info:hover {
  background: #f5f7fa;
}

.user-info .username {
  font-size: 14px;
  color: #606266;
}

.app-main {
  padding-top: calc(var(--header-height) + 20px);
  padding-bottom: 40px;
}

.main-content {
  max-width: 1400px;
  margin: 0 auto;
  width: 100%;
}

/* Transitions */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
