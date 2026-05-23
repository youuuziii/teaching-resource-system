import { createRouter, createWebHistory } from 'vue-router'
import { ElMessage } from 'element-plus'
import AuditView from '../views/AuditView.vue'
import CoursesView from '../views/CoursesView.vue'
import AdminLogsView from '../views/AdminLogsView.vue'
import AdminUsersView from '../views/AdminUsersView.vue'
import AdminConfigView from '../views/AdminConfigView.vue'
import AdminBackupView from '../views/AdminBackupView.vue'
import GraphView from '../views/GraphView.vue'
import LoginView from '../views/LoginView.vue'
import ProfileView from '../views/ProfileView.vue'
import LearningView from '../views/LearningView.vue'
import ResourceDetailView from '../views/ResourceDetailView.vue'
import ResourceListView from '../views/ResourceListView.vue'
import TeacherCoursesView from '../views/TeacherCoursesView.vue'

const STATIC_PATHS = new Set(['/login', '/resources/:id'])

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: ResourceListView },
    { path: '/resources/:id', component: ResourceDetailView },
    { path: '/graph', component: GraphView },
    { path: '/profile', component: ProfileView },
    { path: '/learning', component: LearningView },
    { path: '/login', component: LoginView },
    { path: '/audit', component: AuditView },
    { path: '/courses', component: CoursesView },
    { path: '/admin/logs', component: AdminLogsView },
    { path: '/admin/users', component: AdminUsersView },
    { path: '/admin/config', component: AdminConfigView },
    { path: '/admin/backup', component: AdminBackupView },
    { path: '/teacher/courses', component: TeacherCoursesView },
  ],
})

function getAllowedPages() {
  try {
    const user = JSON.parse(localStorage.getItem('user') || 'null')
    const pages = Array.isArray(user?.pages) ? user.pages : []
    return new Set(pages)
  } catch {
    return new Set()
  }
}

function normalizePath(path) {
  if (path === '/resources' || path.startsWith('/resources/')) return '/resources/:id'
  return path
}

router.beforeEach((to) => {
  if (to.path === '/login') return true
  const token = localStorage.getItem('token')
  if (!token) return { path: '/login' }

  const allowed = getAllowedPages()
  const normalized = normalizePath(to.path)
  if (!allowed.has(normalized) && !STATIC_PATHS.has(normalized)) {
    ElMessage.warning('权限不足，已为您跳转至首页')
    return { path: '/' }
  }
  return true
})

export default router
