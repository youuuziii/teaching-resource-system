import { createRouter, createWebHistory } from 'vue-router'
import GraphView from '../views/GraphView.vue'
import LoginView from '../views/LoginView.vue'
import ProfileView from '../views/ProfileView.vue'
import ResourceListView from '../views/ResourceListView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: ResourceListView },
    { path: '/graph', component: GraphView },
    { path: '/profile', component: ProfileView },
    { path: '/login', component: LoginView },
  ],
})

router.beforeEach((to) => {
  if (to.path === '/login') return true
  if (to.path === '/' || to.path === '/graph') return true
  const token = localStorage.getItem('token')
  if (!token) return { path: '/login' }
  return true
})

export default router
